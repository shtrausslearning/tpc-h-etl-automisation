DROP EXTERNAL TABLE IF EXISTS "user"."customers";
CREATE EXTERNAL TABLE "user"."customers"(
    R_NAME TEXT,
    N_NAME TEXT,
    C_MKTSEGMENT TEXT,
    unique_customers_count BIGINT,
    avg_acctbal FLOAT8,
    mean_acctbal FLOAT8,
    min_acctbal FLOAT8,
    max_acctbal FLOAT8
)
LOCATION ('pxf://de-project/user/customers_report?PROFILE=s3:parquet&SERVER=default')
ON ALL FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') ENCODING 'UTF8';
