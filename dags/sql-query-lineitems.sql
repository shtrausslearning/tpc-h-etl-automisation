DROP EXTERNAL TABLE IF EXISTS "user"."lineitems";
CREATE EXTERNAL TABLE "user"."lineitems"(
    L_ORDERKEY BIGINT,
    count BIGINT,
    sum_extendprice FLOAT8,
    mean_discount FLOAT8,
    mean_tax FLOAT8,
    delivery_days FLOAT8,
    A_return_flags BIGINT,
    R_return_flags BIGINT,
    N_return_flags BIGINT
)
LOCATION ('pxf://de-project/user/lineitems_report?PROFILE=s3:parquet&SERVER=default')
ON ALL FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import') ENCODING 'UTF8';
