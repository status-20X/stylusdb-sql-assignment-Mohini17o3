function parseQuery(query) {
    query = query.trim();

    // Initialize variables for different parts of the query
    let selectPart, fromPart;

    // Split the query at the WHERE clause if it exists
    const whereSplit = query.split(/\sWHERE\s/i);
    query = whereSplit[0]; // Everything before WHERE clause

    // WHERE clause is the second part after splitting, if it exists
    const whereClause = whereSplit.length > 1 ? whereSplit[1].trim() : null;

    // Split the remaining query at the JOIN clause if it exists
    const joinSplit = query.split(/\s(?:INNER|LEFT|RIGHT)\sJOIN\s/i);
    selectPart = joinSplit[0].trim(); // Everything before JOIN clause

    // JOIN clause is the second part after splitting, if it exists
    const joinPart = joinSplit.length > 1 ? joinSplit[1].trim() : null;

    // Parse the SELECT part
    const selectRegex = /^SELECT\s(.+?)\sFROM\s(.+)/i;
    const selectMatch = selectPart.match(selectRegex);
    if (!selectMatch) {
        throw new Error('Invalid SELECT format');
    }

    const [, fields, table] = selectMatch;

    // Parse the JOIN part if it exists
    let joinTable = null, joinCondition = null;
    if (joinPart) {
        const joinClause = parseJoinClause(joinPart);
        joinTable = joinClause.joinTable;
        joinCondition = joinClause.joinCondition;
    }

    // Parse the WHERE part if it exists
    let whereClauses = [];
    if (whereClause) {
        whereClauses = parseWhereClause(whereClause);
    }

    return {
        fields: fields.split(',').map(field => field.trim()),
        table: table.trim(),
        whereClauses,
        joinTable,
        joinCondition
    };
}

function parseWhereClause(whereString) {
    const conditions = whereString.split(/\s+(AND|OR)\s+/i); // Split on logical operators
    return conditions.map(condition => {
        const [field, operator, value] = condition.trim().split(/\s+/); // Split each condition into field, operator, and value
        return { field, operator, value };
    });
}

function parseJoinClause(query) {
    const joinRegex = /\s(INNER|LEFT|RIGHT)\sJOIN\s(.+?)\sON\s([\w.]+)\s*=\s*([\w.]+)/i;
    const joinMatch = query.match(joinRegex);

    if (joinMatch) {
        return {
            joinType: joinMatch[1].trim(),
            joinTable: joinMatch[2].trim(),
            joinCondition: {
                left: joinMatch[3].trim(),
                right: joinMatch[4].trim()
            }
        };
    }

    return {
        joinType: null,
        joinTable: null,
        joinCondition: null
    };
}

module.exports = { parseQuery, parseJoinClause };
