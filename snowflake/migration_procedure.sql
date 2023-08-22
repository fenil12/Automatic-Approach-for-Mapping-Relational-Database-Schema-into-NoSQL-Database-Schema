CREATE OR REPLACE PROCEDURE MIGRATION_PROCEDURE()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS 
$$
var result;
var execution_step;
var error_msg;
var process_name = "migration_procedure";
var execution_status = "Succeeded";
var json_rows1;
label: try
{    

    execution_step = "Creating Stage for lineitem_collection";
    
    var rs = snowflake.execute({
        sqlText: `CREATE OR REPLACE STAGE my_stage1
                      FILE_FORMAT = (TYPE = 'JSON' COMPRESSION = NONE );`
    });
    
    snowflake.execute({
        sqlText: `INSERT INTO SCHEMA_TPCH.PROCESS_LOG(EXECUTION_STEP, EXECUTION_STATUS, PROCESS_NAME) VALUES (?, ?, ?)`,
        binds: [execution_step, execution_status, process_name]
    });

    execution_step = "Copying data from collection to stage for lineitem_collection";
    
    var rs = snowflake.execute({
        sqlText: `COPY INTO @my_stage1
                  FROM (SELECT TO_VARIANT(json_data) FROM PUBLIC.LINEITEM_COLLECTION)
                  FILE_FORMAT = (TYPE = 'JSON' COMPRESSION = NONE)
                  OVERWRITE = TRUE;`
    });

    execution_step = "Creating Stage for customer_collection";
    
    var rs = snowflake.execute({
        sqlText: `CREATE OR REPLACE STAGE my_stage2
                      FILE_FORMAT = (TYPE = 'JSON' COMPRESSION = NONE );`
    });
    
    snowflake.execute({
        sqlText: `INSERT INTO SCHEMA_TPCH.PROCESS_LOG(EXECUTION_STEP, EXECUTION_STATUS, PROCESS_NAME) VALUES (?, ?, ?)`,
        binds: [execution_step, execution_status, process_name]
    });

    execution_step = "Copying data from collection to stage for customer_collection";
    
    var rs = snowflake.execute({
        sqlText: `COPY INTO @my_stage2
                  FROM (SELECT TO_VARIANT(json_data) FROM PUBLIC.CUSTOMER_COLLECTION)
                  FILE_FORMAT = (TYPE = 'JSON' COMPRESSION = NONE)
                  OVERWRITE = TRUE;`
    });

    execution_step = "Creating Stage for orders_collection";
    
    var rs = snowflake.execute({
        sqlText: `CREATE OR REPLACE STAGE my_stage3
                      FILE_FORMAT = (TYPE = 'JSON' COMPRESSION = NONE );`
    });
    
    snowflake.execute({
        sqlText: `INSERT INTO SCHEMA_TPCH.PROCESS_LOG(EXECUTION_STEP, EXECUTION_STATUS, PROCESS_NAME) VALUES (?, ?, ?)`,
        binds: [execution_step, execution_status, process_name]
    });

    execution_step = "Copying data from collection to stage for orders_collection";
    
    var rs = snowflake.execute({
        sqlText: `COPY INTO @my_stage3
                  FROM (SELECT TO_VARIANT(json_data) FROM PUBLIC.ORDERS_COLLECTION)
                  FILE_FORMAT = (TYPE = 'JSON' COMPRESSION = NONE)
                  OVERWRITE = TRUE;`
    });

execution_step = "Creating Stage for partsupp_collection";
    
    var rs = snowflake.execute({
        sqlText: `CREATE OR REPLACE STAGE my_stage4
                      FILE_FORMAT = (TYPE = 'JSON' COMPRESSION = NONE );`
    });
    
    snowflake.execute({
        sqlText: `INSERT INTO SCHEMA_TPCH.PROCESS_LOG(EXECUTION_STEP, EXECUTION_STATUS, PROCESS_NAME) VALUES (?, ?, ?)`,
        binds: [execution_step, execution_status, process_name]
    });

    execution_step = "Copying data from collection to stage for partsupp_collection";
    
    var rs = snowflake.execute({
        sqlText: `COPY INTO @my_stage4
                  FROM (SELECT TO_VARIANT(json_data) FROM PUBLIC.PARTSUPP_COLLECTION)
                  FILE_FORMAT = (TYPE = 'JSON' COMPRESSION = NONE)
                  OVERWRITE = TRUE;`
    });

execution_step = "Creating Stage for part_collection";
    
    var rs = snowflake.execute({
        sqlText: `CREATE OR REPLACE STAGE my_stage5
                      FILE_FORMAT = (TYPE = 'JSON' COMPRESSION = NONE );`
    });
    
    snowflake.execute({
        sqlText: `INSERT INTO SCHEMA_TPCH.PROCESS_LOG(EXECUTION_STEP, EXECUTION_STATUS, PROCESS_NAME) VALUES (?, ?, ?)`,
        binds: [execution_step, execution_status, process_name]
    });

    execution_step = "Copying data from table to stage for part_collection";
    
    var rs = snowflake.execute({
        sqlText: `COPY INTO @my_stage5
                  FROM (SELECT TO_VARIANT(json_data) FROM PUBLIC.PART_COLLECTION)
                  FILE_FORMAT = (TYPE = 'JSON' COMPRESSION = NONE)
                  OVERWRITE = TRUE;`
    });

  execution_step = "Creating Stage for supplier_collection";
    
    var rs = snowflake.execute({
        sqlText: `CREATE OR REPLACE STAGE my_stage6
                      FILE_FORMAT = (TYPE = 'JSON' COMPRESSION = NONE );`
    });
    
    snowflake.execute({
        sqlText: `INSERT INTO SCHEMA_TPCH.PROCESS_LOG(EXECUTION_STEP, EXECUTION_STATUS, PROCESS_NAME) VALUES (?, ?, ?)`,
        binds: [execution_step, execution_status, process_name]
    });

    execution_step = "Copying data from table to stage for supplier_collection";
    
    var rs = snowflake.execute({
        sqlText: `COPY INTO @my_stage6
                  FROM (SELECT TO_VARIANT(json_data) FROM PUBLIC.SUPPLIER_COLLECTION)
                  FILE_FORMAT = (TYPE = 'JSON' COMPRESSION = NONE)
                  OVERWRITE = TRUE;`
    });

    execution_step = "Creating Stage for region_collection";
    
    var rs = snowflake.execute({
        sqlText: `CREATE OR REPLACE STAGE my_stage7
                      FILE_FORMAT = (TYPE = 'JSON' COMPRESSION = NONE );`
    });
    
    snowflake.execute({
        sqlText: `INSERT INTO SCHEMA_TPCH.PROCESS_LOG(EXECUTION_STEP, EXECUTION_STATUS, PROCESS_NAME) VALUES (?, ?, ?)`,
        binds: [execution_step, execution_status, process_name]
    });

    execution_step = "Copying data from table to stage for region_collection";
    
    var rs = snowflake.execute({
        sqlText: `COPY INTO @my_stage7
                  FROM (SELECT TO_VARIANT(json_data) FROM PUBLIC.REGION_COLLECTION)
                  FILE_FORMAT = (TYPE = 'JSON' COMPRESSION = NONE)
                  OVERWRITE = TRUE;`
    });



}

catch (err) {
    execution_status = "Failed";
    error_msg = err.message;
    snowflake.execute({
    sqlText: `insert into SCHEMA_TPCH.PROCESS_LOG(EXECUTION_STEP,EXECUTION_STATUS,PROCESS_NAME,ERROR_CODE,ERROR_STATE,ERROR_MESSAGE,ERROR_STACK_TEXT) VALUES (?,?,?,?,?,?,?)`
    ,binds: [execution_step, execution_status, process_name, err.code, err.state, err.message, err.stackTraceTxt]
    });    

}                                
    
if (execution_status == "Failed")
{
    result = "Failed in step: " + execution_step + "\\nError Message: " + error_msg + "\\nFurther info can be found in Snowflake log table: SCHEMA_TPCH.PROCESS_LOG";
    return result;
}
else
    return execution_status;              
$$;  

CALL MIGRATION_PROCEDURE();