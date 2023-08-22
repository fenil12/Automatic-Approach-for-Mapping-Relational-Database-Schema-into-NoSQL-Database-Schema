CREATE OR REPLACE PROCEDURE SCHEMA_AUTOMATE_REFERENCE()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
// Input parameters
var schema_name = 'SCHEMA_TPCH';
var database_name = 'DATABASE_TPCH';

// Fetch tables with foreign keys
var primaryKeyTables = snowflake.execute({
  sqlText: `SHOW IMPORTED KEYS IN SCHEMA ${database_name}.${schema_name}`
});

// Initialize arrays to store table and column names
var primaryTables = [];
var primaryColumns = [];
var referenceTables = [];
var referenceColumns = [];
var referenceKeys = [[]]; // Initialize as an empty array
var primaryKeys = [[]]; // Initialize as an empty array

// Loop through the fetched tables with foreign keys
while (primaryKeyTables.next()) {
  // Store table and column names in respective arrays
  primaryTables.push(primaryKeyTables.getColumnValue('pk_table_name'));
  primaryColumns.push(primaryKeyTables.getColumnValue('pk_column_name'));
  referenceTables.push(primaryKeyTables.getColumnValue('fk_table_name'));
  referenceColumns.push(primaryKeyTables.getColumnValue('fk_column_name'));

  // Fetch primary keys of the referenced table
  var referenceKeyArray = [];
  var rk = snowflake.execute({
    sqlText: `SHOW PRIMARY KEYS IN TABLE ${primaryKeyTables.getColumnValue('fk_table_name')}`
  });

  // Store primary keys in an array
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

  // Fetch primary keys of the current table
  var primaryKeyArray = [];
  var pk = snowflake.execute({
    sqlText: `SHOW PRIMARY KEYS IN TABLE ${primaryKeyTables.getColumnValue('pk_table_name')}`
  });

  // Store primary keys in an array
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

// Initialize arrays to store SQL statements and processed tables
var createStatements = [];
var processedTables = []; // Array to track processed tables

// Loop through the primaryTables array to create SQL statements
primaryTables.forEach(function(tableName, index) {
  // Check the number of rows in the table
  var rowCountQuery = `
    SELECT COUNT(*) AS ROW_COUNT
    FROM ${database_name}.${schema_name}.${tableName}
  `;
  var rowCountResult = snowflake.execute({ sqlText: rowCountQuery });
  var rowCount = rowCountResult.next() ? rowCountResult.getColumnValue('ROW_COUNT') : 0;

  // If the table has a large number of rows (rowCount >= 10000)
  if (rowCount >= 10000) {
    // Initialize arrays to store column names and reference keys
    var columnList = [];
    var referenceKeyList = [];
    var referenceKeyList2 = [];

    // Query to fetch column names for the current table
    var columnMappingQuery = `
      SELECT COLUMN_NAME, COLUMN_NAME AS COLUMN_ALIAS
      FROM INFORMATION_SCHEMA.COLUMNS
      WHERE TABLE_NAME = '${tableName}'
        AND TABLE_SCHEMA = '${schema_name}'
    `;
    var columnMappingResult = snowflake.execute({ sqlText: columnMappingQuery });

    // Store column names in an array
    while (columnMappingResult.next()) {
      var columnName = columnMappingResult.getColumnValue('COLUMN_NAME');
      var columnAlias = columnMappingResult.getColumnValue('COLUMN_ALIAS');
      columnList.push(`'${columnName}', ${tableName}.${columnAlias}`);
    }

    // Loop through referenceKeys array to create arrays of reference keys
    referenceKeys[index].forEach(function(referenceKey) {
      referenceKeyList.push(`${referenceTables[index]}.${referenceKey}`);
    });

    referenceKeys[index].forEach(function(referenceKey) {
      referenceKeyList2.push(`'${referenceKey}', ${referenceTables[index]}.${referenceKey}`);
    });

    var createStatement = '';
    var joinConditions = [];

    // Check if the table has composite primary key
    if (primaryKeys[index].length > 1) {
      var foreignReferenceKeyArray = [];

      // Fetch foreign reference keys for the table
      var foreignReferenceTable = snowflake.execute({
        sqlText: `SHOW IMPORTED KEYS IN TABLE ${database_name}.${schema_name}.${referenceTables[index]}`
      });

      // Store foreign reference keys in an array
      var referenceForeignKeyMap = {};
      while (foreignReferenceTable.next()) {
        var pkTableName = foreignReferenceTable.getColumnValue('pk_table_name');
        if (pkTableName === tableName) {
          var foreignReferenceKey = foreignReferenceTable.getColumnValue('fk_column_name');
          var keySequence = foreignReferenceTable.getColumnValue('key_sequence');
          referenceForeignKeyMap[keySequence] = foreignReferenceKey;
        }
      }

      for (var i = 1; i <= Object.keys(referenceForeignKeyMap).length; i++) {
        foreignReferenceKeyArray.push(referenceForeignKeyMap[i]);
      }

      // Create join conditions for the foreign reference keys
      for (var i = 0; i < primaryKeys[index].length; i++) {
        joinConditions.push(`${tableName}.${primaryKeys[index][i]} = ${referenceTables[index]}.${foreignReferenceKeyArray[i]}`);
      }

      // Check if the reference key list has only one element
      if (referenceKeyList.length === 1) {
        // Create SQL statement for single reference key
        createStatement = `
          CREATE OR REPLACE TABLE DATABASE_TPCH.PUBLIC.${tableName}_COLLECTION AS
          SELECT 
              OBJECT_INSERT(
                  OBJECT_CONSTRUCT(${columnList.join(', ')}),
                  '${referenceTables[index]}',
                  ARRAY_AGG(${referenceKeyList.join(', ')})
              )::VARIANT AS JSON_DATA
          FROM ${database_name}.${schema_name}.${tableName}
          LEFT JOIN ${database_name}.${schema_name}.${referenceTables[index]} ON ${joinConditions.join(' AND ')}
          GROUP BY ${columnList.join(', ')};
        `;
      } else {
        // Create SQL statement for multiple reference keys
        createStatement = `
          CREATE OR REPLACE TABLE DATABASE_TPCH.PUBLIC.${tableName}_COLLECTION AS
          SELECT 
              OBJECT_INSERT(
                  OBJECT_CONSTRUCT(${columnList.join(', ')}),
                  '${referenceTables[index]}',
                  (
                    ARRAY_AGG(
                      OBJECT_CONSTRUCT(${referenceKeyList2.join(', ')})
                    )
                  )
              )::VARIANT AS JSON_DATA
          FROM ${database_name}.${schema_name}.${tableName}
          LEFT JOIN ${database_name}.${schema_name}.${referenceTables[index]} ON ${joinConditions.join(' AND ')}
          GROUP BY ${columnList.join(', ')};
        `;
      }
    } else {
      // Create join condition for single primary key
      joinConditions.push(`${tableName}.${primaryColumns[index]} = ${referenceTables[index]}.${referenceColumns[index]}`);

      // Check if the reference key list has only one element and the primary key is single
      if (referenceKeyList.length === 1 && primaryKeys[index].length === 1) {
        // Create SQL statement for single reference key and single primary key
        createStatement = `
          CREATE OR REPLACE TABLE DATABASE_TPCH.PUBLIC.${tableName}_COLLECTION AS
          SELECT 
              OBJECT_INSERT(
                  OBJECT_CONSTRUCT(${columnList.join(', ')}),
                  '${referenceTables[index]}',
                  ARRAY_AGG(${referenceKeyList.join(', ')})
              )::VARIANT AS JSON_DATA
          FROM ${database_name}.${schema_name}.${tableName}
          LEFT JOIN ${database_name}.${schema_name}.${referenceTables[index]} ON ${joinConditions.join(' AND ')}
          GROUP BY ${columnList.join(', ')};
        `;
      } else if (referenceKeyList.length > 1 && primaryKeys[index].length === 1) {
        // Create SQL statement for multiple reference keys and single primary key
        createStatement = `
          CREATE OR REPLACE TABLE DATABASE_TPCH.PUBLIC.${tableName}_COLLECTION AS
          SELECT 
              OBJECT_INSERT(
                  OBJECT_CONSTRUCT(${columnList.join(', ')}),
                  '${referenceTables[index]}',
                  (
                    ARRAY_AGG(
                      OBJECT_CONSTRUCT(${referenceKeyList2.join(', ')})
                    )
                  )
              )::VARIANT AS JSON_DATA
          FROM ${database_name}.${schema_name}.${tableName}
          LEFT JOIN ${database_name}.${schema_name}.${referenceTables[index]} ON ${joinConditions.join(' AND ')}
          GROUP BY ${columnList.join(', ')};
        `;
      }
    }

    // Check if the createStatement is not empty and the table has not been processed before
    if (createStatement !== '' && !processedTables.includes(tableName)) {
      createStatements.push(createStatement);
      processedTables.push(tableName);
    }
  }
});

// Execute each SQL statement in the createStatements array using a loop
createStatements.forEach(function(statement) {
  var statementResult = snowflake.execute({
    sqlText: statement
  });
});

// Return the createStatements array as the output
return createStatements;
$$;

// Call the procedure to execute the schema automation
CALL SCHEMA_AUTOMATE_REFERENCE();