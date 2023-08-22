CREATE OR REPLACE PROCEDURE MASTER_PROCEDURE_AUTOMATE()
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
var result;
var execution_step;
var error_msg;
var process_name = "master_procedure_automate";
var execution_status = "Succeeded";
var json_rows1;
label: try
{    

    execution_step = "calling schema_automate_independent procedure";
    snowflake.execute({
        sqlText: `insert into SCHEMA_TPCH.PROCESS_LOG(EXECUTION_STEP,EXECUTION_STATUS,PROCESS_NAME) VALUES (?,?,?)`
        ,binds: [execution_step, execution_status, process_name]
    });    

    var query = ''call schema_tpch.schema_automate_independent()'';
    var rs = snowflake.execute( { sqlText:query });
    while(rs.next())
        json_rows1 = rs.getColumnValue(1);
    if (json_rows1 == "Failed")
    {
        execution_status = "Failed";
        break label;
    }    

    execution_step = "calling schema_automate_reference procedure";    
    snowflake.execute({
        sqlText: `insert into SCHEMA_TPCH.PROCESS_LOG(EXECUTION_STEP,EXECUTION_STATUS,PROCESS_NAME) VALUES (?,?,?)`
        ,binds: [execution_step, execution_status, process_name]
    });    

    var query = ''call schema_tpch.schema_automate_reference()'';
    var rs = snowflake.execute( { sqlText:query });
    while(rs.next())
        json_rows1 = rs.getColumnValue(1);
    if (json_rows1 == "Failed")
    {
        execution_status = "Failed";
        break label;
    }

    execution_step = "calling schema_automate_embedded procedure";
    snowflake.execute({
        sqlText: `insert into SCHEMA_TPCH.PROCESS_LOG(EXECUTION_STEP,EXECUTION_STATUS,PROCESS_NAME) VALUES (?,?,?)`
        ,binds: [execution_step, execution_status, process_name]
    });    

    var query = ''call schema_tpch.schema_automate_embedded()'';
    var rs = snowflake.execute( { sqlText:query });
    while(rs.next())
        json_rows1 = rs.getColumnValue(1);
    if (json_rows1 == "Failed")
    {
        execution_status = "Failed";
        break label;
    }


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
';  

CALL MASTER_PROCEDURE_AUTOMATE();
