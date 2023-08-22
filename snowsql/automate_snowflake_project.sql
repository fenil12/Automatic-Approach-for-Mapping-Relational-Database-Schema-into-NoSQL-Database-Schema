USE DATABASE DATABASE_TPCH;
USE SCHEMA SCHEMA_TPCH;
GET @my_stage1 file://c:/fenil/snowflake_project/lineitem;
GET @my_stage2 file://c:/fenil/snowflake_project/customer;
GET @my_stage3 file://c:/fenil/snowflake_project/orders;
GET @my_stage4 file://c:/fenil/snowflake_project/partsupp;
GET @my_stage5 file://c:/fenil/snowflake_project/part;
GET @my_stage6 file://c:/fenil/snowflake_project/supplier;
GET @my_stage7 file://c:/fenil/snowflake_project/region;

