--CREATING COLLECTION WHICH DOESN'T HAS FOREIGN KEYS means that table isnt been referenced by other table
CREATE OR REPLACE PROCEDURE SCHEMA_AUTOMATE_INDEPENDENT()
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
var schema_name = 'SCHEMA_TPCH';
var database_name = 'DATABASE_TPCH';

// Get tables with primary key
var primaryKeyTables = snowflake.execute({
  sqlText: `SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
            WHERE CONSTRAINT_TYPE = 'PRIMARY KEY'
              AND TABLE_SCHEMA = '${schema_name}'`
});

var primaryKeys = [];
while (primaryKeyTables.next()) {
  primaryKeys.push(primaryKeyTables.getColumnValue('TABLE_NAME'));
}

// Get foreign key tables
var foreignKeyTables = snowflake.execute({
  sqlText: `SHOW IMPORTED KEYS IN SCHEMA ${database_name}.${schema_name}`
});

var foreignKeys = [];
while (foreignKeyTables.next()) {
  foreignKeys.push(foreignKeyTables.getColumnValue('pk_table_name'));
}

var createStatements = [];

// Check if a table's primary key is not referenced as a foreign key
primaryKeys.forEach(function(tableName) {
  if (!foreignKeys.includes(tableName)) {
    var createStatement = `
      CREATE OR REPLACE TABLE DATABASE_TPCH.PUBLIC.${tableName}_COLLECTION AS
      SELECT OBJECT_CONSTRUCT(*)::VARIANT AS JSON_DATA
      FROM ${database_name}.${schema_name}.${tableName}
    `;
  
    createStatements.push(createStatement);
  }
});

createStatements.forEach(function(statement) {
  var statementResult = snowflake.execute({
    sqlText: statement
  });
});

return createStatements;
$$;

CALL SCHEMA_AUTOMATE_INDEPENDENT();
