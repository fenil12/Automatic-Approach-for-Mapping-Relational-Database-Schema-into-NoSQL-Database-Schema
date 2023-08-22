CREATE OR REPLACE PROCEDURE SCHEMA_AUTOMATE_EMBEDDED()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
// Constants for schema and database names
var schema_name = 'SCHEMA_TPCH';
var database_name = 'DATABASE_TPCH';

// Retrieve the information of primary and reference key relationships
var primaryKeyTables = snowflake.execute({
  sqlText: `SHOW IMPORTED KEYS IN SCHEMA ${database_name}.${schema_name}`
});

var primaryTables = [];
var primaryColumns = [];
var referenceTables = [];
var referenceColumns = [];
var referenceKeys = [[]]; // Initialize as an empty array
var primaryKeys = [[]]; // Initialize as an empty array

// Loop through the primary key relationships to gather information
while (primaryKeyTables.next()) {
  primaryTables.push(primaryKeyTables.getColumnValue('pk_table_name'));
  primaryColumns.push(primaryKeyTables.getColumnValue('pk_column_name'));
  referenceTables.push(primaryKeyTables.getColumnValue('fk_table_name'));
  referenceColumns.push(primaryKeyTables.getColumnValue('fk_column_name'));

  var referenceKeyArray = [];
  var primaryKeyArray = [];


  // Fetch the reference key information
  var rk = snowflake.execute({
    sqlText: `SHOW PRIMARY KEYS IN TABLE ${primaryKeyTables.getColumnValue('fk_table_name')}`
  });

  var referenceKeyMap = {};
  while (rk.next()) {
    var referenceKey = rk.getColumnValue('column_name');
    var keySequence = rk.getColumnValue('key_sequence');
    referenceKeyMap[keySequence] = referenceKey;
  }

  for (var i = 1; i <= Object.keys(referenceKeyMap).length; i++) {
    referenceKeyArray.push(referenceKeyMap[i]);
  }

  referenceKeys.push(referenceKeyArray);

  // Fetch the primary key information
  var pk = snowflake.execute({
    sqlText: `SHOW PRIMARY KEYS IN TABLE ${primaryKeyTables.getColumnValue('pk_table_name')}`
  });

  var primaryKeyMap = {};
  while (pk.next()) {
    var primaryKey = pk.getColumnValue('column_name');
    var keySequence = pk.getColumnValue('key_sequence');
    primaryKeyMap[keySequence] = primaryKey;
  }

  for (var i = 1; i <= Object.keys(primaryKeyMap).length; i++) {
    primaryKeyArray.push(primaryKeyMap[i]);
  }

  primaryKeys.push(primaryKeyArray);
}

referenceKeys.shift(); //Remove the initial empty array
primaryKeys.shift(); //Remove the initial empty array

// Array to store create table statements for tables with less than 10,000 records
var createStatements = [];
var processedTables = []; // Array to track processed tables

//variable created for embedded document
var pt = [];
var ft = [];
var columnList = [];
var referenceKeyList = [];
var referenceKeyList2 = [];

// Loop through each primary table to process
primaryTables.forEach(function(tableName, index) {
  var rowCountQuery = `
    SELECT COUNT(*) AS ROW_COUNT
    FROM ${database_name}.${schema_name}.${tableName}
  `;

  // Execute the row count query to get the number of records in the table
  var rowCountResult = snowflake.execute({ sqlText: rowCountQuery });
  var rowCount = rowCountResult.next() ? rowCountResult.getColumnValue('ROW_COUNT') : 0;

  
  // Check if the table has less than 10,000 records
  if (rowCount < 10000) {
    pt.push(tableName);
    ft.push(referenceTables[index]);
    //var columnList = [];
    //var referenceKeyList = [];
    //var referenceKeyList2 = [];
    var columnMappingQuery = `
      SELECT COLUMN_NAME, COLUMN_NAME AS COLUMN_ALIAS
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_NAME = '${tableName}'
        AND TABLE_SCHEMA = '${schema_name}'
      ORDER BY ORDINAL_POSITION;
    `;

    // Fetch column information for constructing the queries
    var columnMappingResult = snowflake.execute({ sqlText: columnMappingQuery });
    while (columnMappingResult.next()) {
      var columnName = columnMappingResult.getColumnValue('COLUMN_NAME');
      var columnAlias = columnMappingResult.getColumnValue('COLUMN_ALIAS');
      columnList.push(`'${columnName}', ${tableName}.${columnAlias}`);
    }

    //for 1 value of reference key
    referenceKeys[index].forEach(function(referenceKey) {
      referenceKeyList.push(`${referenceTables[index]}.${referenceKey}`);
    });

    //for more than 2 values of reference keys
    referenceKeys[index].forEach(function(referenceKey) {
      referenceKeyList2.push(`'${referenceKey}', ${referenceTables[index]}.${referenceKey}`);
    });
  }
});

// Helper function to check if a table has at least 10,000 records
function hasMinimumRecords(table) {
  var rowCountQuery = `
    SELECT COUNT(*) AS ROW_COUNT
    FROM ${database_name}.${schema_name}.${table}
  `;
  var rowCountResult = snowflake.execute({ sqlText: rowCountQuery });
  var rowCount = rowCountResult.next() ? rowCountResult.getColumnValue('ROW_COUNT') : 0;
  return rowCount >= 10000;
}

// Array to store the final collection of tables and their reference tables
var finalCollection = [];
var ptObject = {};
var f1 = [];
var f2 = [];

// Create a Set to remove duplicates from columnList
var uniqueColumns = new Set(columnList);

// Convert the Set back to an array
columnList = Array.from(uniqueColumns);


// Group the reference tables (ft values) based on the ptValue
pt.forEach(function (ptValue, index) {
  var combinedColumns = [];
  var embeddedTable = ft[index];

  if (ptObject.hasOwnProperty(ptValue)) {
    // Combine the columns for the current reference table with the existing columns
    combinedColumns = ptObject[ptValue][0].columns.concat(columnList.filter(column => column.includes(`${ptValue}.`)));
    ptObject[ptValue][0].referenceTable += `, ${ft[index]}`; // Combine the reference tables
  } else {
    // Create a new entry for ptValue with reference table and columns
    combinedColumns = columnList.filter(column => column.includes(`${ptValue}.`));
    ptObject[ptValue] = [{
      referenceTable: ft[index],
      columns: combinedColumns,
	  embeddedTable: ''
    }];
  }
});

var refTableList = '';
var updatedRefTableList = [];

// Modify the ptObject to include embeddedTable information
Object.keys(ptObject).forEach(function (ptValue) {
  refTableList = ptObject[ptValue][0].referenceTable.split(', ');

  if (refTableList.length > 1) {
    ptObject[ptValue][0].embeddedTable = '';
    ptObject[ptValue][0].referenceTable = '';
    updatedRefTableList = [];

    refTableList.forEach(function (refTable) {
      if (ptObject.hasOwnProperty(refTable)) {
        ptObject[ptValue][0].embeddedTable = refTable;
      } else {
        updatedRefTableList.push(refTable);
      }
    });
    ptObject[ptValue][0].referenceTable = updatedRefTableList.join(', ');
  }
  else {
    ptObject[ptValue][0].embeddedTable = '';
    ptObject[ptValue][0].referenceTable = '';
    updatedRefTableList = [];

    refTableList.forEach(function (refTable) {
      if (ptObject.hasOwnProperty(refTable)) {
        ptObject[ptValue][0].embeddedTable = refTable;
      } 
      else {
        updatedRefTableList.push(refTable);
        }
    ptObject[ptValue][0].referenceTable = updatedRefTableList.join(', ');
    });
  }
});

//NOW THE QUERY STATEMENT TO BUILD

// Object to store the final reference statements for each primary table
var referenceStatements = {};

// Iterate through ptObject to construct reference statements for each primary table
Object.keys(ptObject).forEach(function (ptValue) {
  var refStatement = constructReferenceStatement(ptValue);
  if (refStatement !== '') {
    referenceStatements[ptValue] = refStatement;
  }
});

var rtt;

// Helper function to construct the reference statement for a given primary table where there is no contruction for tables containing //embedded value
function constructReferenceStatement(primaryTable) {
  var refStatement = '';

  if (ptObject[primaryTable]) {
    var embeddedTable = ptObject[primaryTable][0].embeddedTable;
    var referenceTable = ptObject[primaryTable][0].referenceTable.split(', ');
    
    // Fetch primary key information for primaryTable from ptObject
    var newpkResult = snowflake.execute({
      sqlText: `SHOW PRIMARY KEYS IN TABLE ${primaryTable}`
    });

    var newpk = [];
    while (newpkResult.next()) {
      newpk.push(newpkResult.getColumnValue('column_name'));
    }

    // Fetch foreign key information for embeddedTable from ptObject
    var newekResult = snowflake.execute({
      sqlText: `SHOW IMPORTED KEYS IN TABLE ${embeddedTable}`
    });

    var newek = [];
    while (newekResult.next()) {
      newek.push(newekResult.getColumnValue('fk_column_name'));
    }

    // Fetch primary key information for referenceTable from ptObject
    var newfkr = []; // Initialize an empty array to store the primary key column names

    if (Array.isArray(referenceTable)) {
      referenceTable.forEach(function (table) {
        var newfkrResult = snowflake.execute({
          sqlText: `SHOW PRIMARY KEYS IN TABLE ${table}`
        });
    
        while (newfkrResult.next()) {
          newfkr.push(newfkrResult.getColumnValue('column_name'));
        }
      });
    } else {
      var newfkrResult = snowflake.execute({
        sqlText: `SHOW PRIMARY KEYS IN TABLE ${referenceTable}`
      });
    
      while (newfkrResult.next()) {
        newfkr.push(newfkrResult.getColumnValue('column_name'));
      }
    }


    // Fetch foreign key information for referenceTable from ptObject
    var newfk = [];

    if (Array.isArray(referenceTable)) {
      referenceTable.forEach(function (table) {
        var newfkResult = snowflake.execute({
          sqlText: `SHOW IMPORTED KEYS IN TABLE ${table}` // Fixed the SQL query here
        });
        
        while (newfkResult.next()) {
          newfk.push(newfkResult.getColumnValue('fk_column_name'));
        }
      });
    } else {
      var newfkResult = snowflake.execute({
        sqlText: `SHOW IMPORTED KEYS IN TABLE ${referenceTable}` // Fixed the SQL query here
      });
    
      while (newfkResult.next()) {
        newfk.push(newfkResult.getColumnValue('fk_column_name'));
      }
    }

    // Create the join conditions using newpk = newfk
    //var joinConditions = newpk.map((column, index) => `${primaryTable}.${column} = ${referenceTable}.${newfk[index]}`).join(' AND ');
    // Create the join conditions using newpk = newfk
    //var joinConditions = newpk.map((column, index) => `${primaryTable}.${column} = ${newfk[index]}`).join(' AND ');

    var joinConditions = [];

    if (Array.isArray(referenceTable)) {
      referenceTable.forEach(function (table, tableIndex) {
        var joinCondition = newpk.map((column, index) => `${primaryTable}.${column} = ${table}.${newfk[tableIndex]}`);
        joinConditions.push(joinCondition.join(' AND '));
      });
    } else {
      joinConditions.push(newpk.map((column, index) => `${primaryTable}.${column} = ${referenceTable}.${newfk[index]}`).join(' AND '));
    }

    var embjoinConditions = [];
    embjoinConditions.push(newpk.map((column, index) => `${primaryTable}.${column} = ${embeddedTable}.${newek[index]}`).join(' AND '));
    
    //var aggConditions = [];
    //var aggConditions.push(${newfk.map(column => `'${column}', ${referenceTable}.${column}`).join(', ')});

    var aggConditions = [];

    if (Array.isArray(referenceTable)) {
      referenceTable.forEach((table, tableIndex) => {
        var newFkValues = [];
        newFkValues.push(`${table}.${newfk[tableIndex]}`);
        aggConditions.push(newFkValues.join(', '));
      });
    } else {
      var newFkValues = newfk.map(column => `${referenceTable}.${column}`);
      aggConditions.push(newFkValues.join(', '));
    }


    if (embeddedTable === '' && ptObject[primaryTable][0].referenceTable !== '') 
    {
        if(referenceTable.length === 1)
      {
          refStatement = `
            OBJECT_INSERT(
              OBJECT_CONSTRUCT(${ptObject[primaryTable][0].columns.join(', ')}),
              '${referenceTable}',
              (
                SELECT ARRAY_AGG(
                  OBJECT_CONSTRUCT(${newfk.map(column => `'${column}', ${referenceTable}.${column}`).join(', ')})
                )
                FROM ${database_name}.${schema_name}.${referenceTable}
                WHERE ${joinConditions}
              )
            )
          `;
      }
      //when reference table contains 2 tables
      else
      {
          //rtt = newfkr;
          refStatement = `
                OBJECT_INSERT(
                    OBJECT_INSERT(
                      OBJECT_CONSTRUCT(${ptObject[primaryTable][0].columns.join(', ')}),
                      '${referenceTable[0]}',
                      (
                        SELECT ARRAY_AGG(${newfkr[0]})
                        FROM ${database_name}.${schema_name}.${referenceTable[0]}
                        WHERE ${joinConditions[0]}
                      )
                      ),
                      '${referenceTable[1]}',
                      (
                        SELECT ARRAY_AGG(${newfkr[1]})
                        FROM ${database_name}.${schema_name}.${referenceTable[1]}
                        WHERE ${joinConditions[1]}
                      )
                    )
              `;
      }
    }
    else if(embeddedTable !== '' && ptObject[primaryTable][0].referenceTable === '')
    {
        if(referenceStatements.hasOwnProperty(embeddedTable))
        {
            //rtt = embjoinConditions;
            // Fetch all column names of the embeddedTable
            var embColumnsResult = snowflake.execute({
                sqlText: `SHOW COLUMNS IN TABLE ${embeddedTable}`
            });

            var embColumns = [];
            while (embColumnsResult.next()) {
                embColumns.push(embColumnsResult.getColumnValue('column_name'));
            }
            
            refStatement = `
                CREATE OR REPLACE TABLE DATABASE_TPCH.PUBLIC.${primaryTable}_COLLECTION AS
                SELECT
                OBJECT_INSERT(
                  OBJECT_CONSTRUCT(${ptObject[primaryTable][0].columns.join(', ')}),
                  '${embeddedTable}',
                  ${referenceStatements[embeddedTable]}
                )::VARIANT AS JSON_DATA
                FROM ${database_name}.${schema_name}.${primaryTable}
                JOIN ${database_name}.${schema_name}.${embeddedTable} ON ${embjoinConditions.join(' AND ')}
                GROUP BY ${ptObject[primaryTable][0].columns.join(', ')}, ${embColumns.join(', ')}
            `;
        }    
   }
 }   
    // ... (Other conditions and logic for refStatement)
    return refStatement;
}

// Function to find referenceStatements key values that don't match with referenceTable and embeddedTable values
function findNonMatchingKeys(referenceStatements, ptObject) {
  var nonMatchingKeys = [];

  // Get all referenceTable and embeddedTable values from ptObject
  var referenceTableValues = Object.values(ptObject).map(entry => entry[0].referenceTable);
  var embeddedTableValues = Object.values(ptObject).map(entry => entry[0].embeddedTable);
  
  // Check if any key in referenceStatements doesn't match referenceTable or embeddedTable value
  for (var key of Object.keys(referenceStatements)) {
    if (!referenceTableValues.includes(key) && !embeddedTableValues.includes(key)) {
      nonMatchingKeys.push(key);
    }
  }

  return nonMatchingKeys;
}

// Call the function to find non-matching keys
var nonMatchingKeys = findNonMatchingKeys(referenceStatements, ptObject);

// Create an array to store matching referenceStatements values
var createStatements = [];

// Iterate over nonMatchingKeys and compare with referenceStatements keys
for (var key of nonMatchingKeys) {
  if (referenceStatements.hasOwnProperty(key)) {
    // If the key is present in referenceStatements, add the value to createStatements
    createStatements.push(referenceStatements[key]);
  }
}

createStatements.forEach(function(statement) {
  var statementResult = snowflake.execute({
    sqlText: statement
  });
});

return createStatements;
$$;

CALL SCHEMA_AUTOMATE_EMBEDDED();

SELECT COLUMN_NAME, COLUMN_NAME AS COLUMN_ALIAS
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'NATION'
AND TABLE_SCHEMA = 'SCHEMA_TPCH'
ORDER BY ORDINAL_POSITION;

SHOW COLUMNS IN TABLE NATION;
