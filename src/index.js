// src/index.js

// Import necessary modules
const {parseQuery} = require('./queryParser');
const readCSV = require('./csvReader');

// Helper function for INNER JOIN
function performInnerJoin(mainData, joinData, joinCondition, fields, mainTable) {
    const result = [];
    mainData.forEach(mainRow => {
        joinData.forEach(joinRow => {
            if (mainRow[joinCondition.left] === joinRow[joinCondition.right]) {
                const combinedRow = { ...mainRow, ...joinRow };
                delete combinedRow[joinCondition.right]; // Remove redundant column
                result.push(combinedRow);
            }
        });
    });
    return result;
}

// Helper function for LEFT JOIN
function performLeftJoin(mainData, joinData, joinCondition, fields, mainTable) {
    return mainData.map(mainRow => {
        const matchingRows = joinData.filter(joinRow => joinRow[joinCondition.right] === mainRow[joinCondition.left]);
        if (matchingRows.length === 0) {
            // No match found, include mainRow with null values for joinData
            return { ...mainRow, [mainTable]: null };
        } else {
            // Match found, include mainRow with matching joinData
            return matchingRows.map(matchingRow => ({ ...mainRow, [mainTable]: matchingRow }));
        }
    }).flat(); // Flatten the array of arrays into a single array
}

// Helper function for RIGHT JOIN
function performRightJoin(mainData, joinData, joinCondition, fields, mainTable) {
    return joinData.map(joinRow => {
        const matchingRows = mainData.filter(mainRow => mainRow[joinCondition.left] === joinRow[joinCondition.right]);
        if (matchingRows.length === 0) {
            // No match found, include joinRow with null values for mainData
            return { ...joinRow, [mainTable]: null };
        } else {
            // Match found, include joinRow with matching mainData
            return { ...joinRow, [mainTable]: matchingRows };
        }
    });
}
// Main function to execute SELECT query
async function executeSELECTQuery(query) {
    const { fields, table, whereClauses, joinType, joinTable, joinCondition } = parseQuery(query);
    let data = await readCSV(`${table}.csv`);

    // Apply JOIN logic
    if (joinTable && joinCondition) {
        const joinData = await readCSV(`${joinTable}.csv`);
        switch (joinType.toUpperCase()) {
            case 'INNER':
                data = performInnerJoin(data, joinData, joinCondition, fields, table);
                break;
            case 'LEFT':
                data = performLeftJoin(data, joinData, joinCondition, fields, table);
                break;
            case 'RIGHT':
                data = performRightJoin(data, joinData, joinCondition, fields, table);
                break;
            default:
                throw new Error(`Unsupported JOIN type: ${joinType}`);
        }
    }

    // Apply WHERE clause filtering
    const filteredData = whereClauses.length > 0 ? data.filter(row => whereClauses.every(clause => evaluateCondition(row, clause))) : data;

    // Select the specified fields
    return filteredData.map(row => {
        const selectedRow = {};
        fields.forEach(field => {
            // Assuming 'field' is just the column name without table prefix
            selectedRow[field] = row[field];
        });
        return selectedRow;
    });
}

// Function to evaluate WHERE clause condition
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

// Export the executeSELECTQuery function
module.exports = executeSELECTQuery;
