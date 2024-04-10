// const parseQuery = require('./queryParser');
const { parseQuery, parseJoinClause } = require('./queryParser.js');


const readCSV = require('./csvReader');

// Helper functions for different JOIN types
// Helper functions for different JOIN types
// function performInnerJoin(mainData, joinData, joinCondition) {
//     // Logic for INNER JOIN
//     return mainData.flatMap(mainRow => {
//         return joinData
//             .filter(joinRow => {
//                 const mainValue = mainRow[joinCondition.left.split('.')[1]];
//                 const joinValue = joinRow[joinCondition.right.split('.')[1]];
//                 return mainValue === joinValue;
//             })
//             .map(joinRow => {
//                 return { ...mainRow, ...joinRow };
//             });
//     });
// }
function performInnerJoin(
  data,
  joinData,
  joinCondition,
  fields,
  leftTableName
) {
  // Logic for INNER JOIN
  return data.flatMap((leftTableRow) => {
    return joinData
      .filter((rightTableRow) => {
        const leftValue = leftTableRow[joinCondition.left.split(".")[1]];
        const rightValue = rightTableRow[joinCondition.right.split(".")[1]];
        return leftValue === rightValue;
      })
      .map((rightTableRow) => {
        return fields.reduce((acc, field) => {
          const [tableName, fieldName] = field.split(".");
          acc[field] =
            tableName === leftTableName
              ? leftTableRow[fieldName]
              : rightTableRow[fieldName];
          return acc;
        }, {});
      });
  });
}

// function performLeeeftJoin(mainData, joinData, joinCondition) {
//     // Logic for LEFT JOIN
//     return mainData.flatMap(mainRow => {
//         const matchingJoinRows = joinData.filter(joinRow => {
//             const mainValue = mainRow[joinCondition.left.split('.')[1]];
//             const joinValue = joinRow[joinCondition.right.split('.')[1]];
//             return mainValue === joinValue;
//         });

//         return matchingJoinRows.length > 0
//             ? matchingJoinRows.map(joinRow => ({ ...mainRow, ...joinRow }))
//             : [{ ...mainRow, [joinCondition.right.split('.')[1]]: null }];
//     });
// }

function performLeftJoin(data, joinData, joinCondition, fields, leftTableName) {
  // Logic for LEFT JOIN

  return data.flatMap((leftTableRow) => {
    const filteredData = joinData.filter((rightTableRow) => {
      const leftValue = leftTableRow[joinCondition.left.split(".")[1]];
      const rightValue = rightTableRow[joinCondition.right.split(".")[1]];
      return leftValue === rightValue;
    });

    if (filteredData.length == 0) {
      return fields.reduce((acc, field) => {
        const [tableName, fieldName] = field.split(".");
        acc[field] =
          tableName === leftTableName ? leftTableRow[fieldName] : null;

        return acc;
      }, {});
    }

    return filteredData.map((rightTableRow) => {
      return fields.reduce((acc, field) => {
        const [tableName, fieldName] = field.split(".");

        acc[field] =
          tableName === leftTableName
            ? leftTableRow[fieldName]
            : rightTableRow[fieldName];

        return acc;
      }, {});
    });
  });
}


// function performRiiightJoin(mainData, joinData, joinCondition) {
//     // Logic for RIGHT JOIN
//     return joinData.flatMap(joinRow => {
//         const matchingMainRows = mainData.filter(mainRow => {
//             const mainValue = mainRow[joinCondition.left.split('.')[1]];
//             const joinValue = joinRow[joinCondition.right.split('.')[1]];
//             return mainValue === joinValue;
//         });

//         return matchingMainRows.map(mainRow => ({ ...mainRow, ...joinRow }));
//     });
// }

// const performLeeeftJoin = require("./performLeftJoin");

function performRightJoin(
  data,
  joinData,
  joinCondition,
  fields,
  rightTableName
) {
  return joinData.flatMap((rightTableRow) => {
    const filteredData = data.filter((leftTableRow) => {
      const leftValue = leftTableRow[joinCondition.left.split(".")[1]];
      const rightValue = rightTableRow[joinCondition.right.split(".")[1]];
      return leftValue === rightValue;
    });

    if (filteredData.length == 0) {
      return fields.reduce((acc, field) => {
        const [tableName, fieldName] = field.split(".");
        acc[field] =
          tableName === rightTableName ? rightTableRow[fieldName] : null;

        return acc;
      }, {});
    }

    return filteredData.map((leftTableRow) => {
      return fields.reduce((acc, field) => {
        const [tableName, fieldName] = field.split(".");

        acc[field] =
          tableName === rightTableName
            ? rightTableRow[fieldName]
            : leftTableRow[fieldName];

        return acc;
      }, {});
    });
  });
}

async function executeSELECTQuery(query, joinType) {


// Now we will have joinTable, joinCondition in the parsed query
const { fields, table, whereClauses, joinTable, joinCondition } = parseQuery(query);
let data = await readCSV(`${table}.csv`);
// let data = await executeSELECTQuery(query, joinType);


// Perform INNER JOIN if specified
// if (joinTable && joinCondition) {
//     const joinData = await readCSV(`${joinTable}.csv`);
//     data = data.flatMap(mainRow => {
//         return joinData
//             .filter(joinRow => {
//                 const mainValue = mainRow[joinCondition.left.split('.')[1]];
//                 const joinValue = joinRow[joinCondition.right.split('.')[1]];
//                 return mainValue === joinValue;
//             })
//             .map(joinRow => {
//                 return fields.reduce((acc, field) => {
//                     const [tableName, fieldName] = field.split('.');
//                     acc[field] = tableName === table ? mainRow[fieldName] : joinRow[fieldName];
//                     return acc;
//                 }, {});
//             });
//     });
// }
 // Logic for applying JOINs
 if (joinTable && joinCondition) {
    
    const joinData = await readCSV(`${joinTable}.csv`);
    // switch (joinType.toUpperCase()) 
    switch (joinType ? joinType.toUpperCase() : '')
    
    {
        case 'INNER':
            data = performInnerJoin(data, joinData, joinCondition, fields, table);
            break;
        case 'LEFT':
            data = performLeftJoin(data, joinData, joinCondition, fields, table);
            break;
        case 'RIGHT':
            data = performRightJoin(data, joinData, joinCondition, fields, table);
            break;
        // Handle default case or unsupported JOIN types
           default:
              throw new Error(`Unsupported join type: ${joinType}`);
        // //  if (typeof joinType === 'undefined') {
           
        //     throw new Error('Join type is not defined');
        // }
         
    }
}





// Apply WHERE clause filtering after JOIN (or on the original data if no join)
const filteredData = whereClauses.length > 0
    ? data.filter(row => whereClauses.every(clause => evaluateCondition(row, clause)))
    : data;
// src/index.js at executeSELECTQuery

return filteredData.map(row => {
    const selectedRow = {};
    fields.forEach(field => {
        // Assuming 'field' is just the column name without table prefix
        selectedRow[field] = row[field];
    });
    return selectedRow;
}

);
}

function evaluateCondition(row, clause) {
    const { field, operator, value } = clause;
    switch (operator) {
        case '=': return row[field] === value;
        case '!=': return row[field] !== value;
        case '>': return row[field] > value;
        case '<': return row[field] < value;
        case '>=': return row[field] >= value;
        case '<=': return row[field] <= value;
        default: throw new Error(`Unsupported operator: ${operator}`);
    }
}

module.exports = executeSELECTQuery;


