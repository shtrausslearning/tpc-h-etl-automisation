import uuid
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType

CUSTOMER_DATA_PATH = f's3a://de-raw/customer'
NATION_DATA_PATH = f's3a://de-raw/nation'
REGION_DATA_PATH = f's3a://de-raw/region'
TARGET_PATH = f's3a://de-project/user/customers_report'


def _spark_session():
	return (SparkSession.builder
			.appName('SparkJob1-' + uuid.uuid4().hex)
			.config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.2') \
			.config('spark.hadoop.fs.s3a.endpoint', '...')
			.config('spark.hadoop.fs.s3a.region', '...')
			.config('spark.hadoop.fs.s3a.access.key', '...')
			.config('spark.hadoop.fs.s3a.secret.key', '...')
			.getOrCreate())

def process(spark: SparkSession):
	
	customer_schema = StructType([
		StructField('C_CUSTKEY', IntegerType(), True),
		StructField('C_NAME', StringType(), True),
		StructField('C_ADDRESS', StringType(), True),
		StructField('C_NATIONKEY', IntegerType(), True),
		StructField('C_PHONE', StringType(), True),
		StructField('C_ACCTBAL', DoubleType(), True),
		StructField('C_MKTSEGMENT', StringType(), True),
		StructField('C_COMMENT', StringType(), True),
	])
	
	customer_fact = spark.read \
		.schema(customer_schema) \
		.parquet(CUSTOMER_DATA_PATH)
		
	nation_schema = StructType([
		StructField('N_NATIONKEY', LongType(), True),
		StructField('N_NAME', StringType(), True),
		StructField('N_REGIONKEY', LongType(), True),
		StructField('N_COMMENT', StringType(), True),
	])

	nation_fact = spark.read \
		.schema(nation_schema) \
		.parquet(NATION_DATA_PATH)
		
	region_schema = StructType([
		StructField('R_REGIONKEY', LongType(), True),
		StructField('R_NAME', StringType(), True),
		StructField('R_COMMENT', StringType(), True),
	])
	
	region_fact = spark.read \
		.schema(region_schema) \
		.parquet(REGION_DATA_PATH)

	datamart = customer_fact \
		.join(other=nation_fact, on=nation_fact['N_NATIONKEY'] == customer_fact['C_NATIONKEY'], how='left') \
		.join(other=region_fact, on=region_fact['R_REGIONKEY'] == nation_fact['N_REGIONKEY'], how='left') \
		.groupBy(nation_fact['N_NAME'], region_fact['R_NAME'], customer_fact['C_MKTSEGMENT']) \
		.agg(F.count(customer_fact['C_CUSTKEY']).alias('unique_customers_count'),
			F.avg(customer_fact['C_ACCTBAL']).alias('avg_acctbal'),
			F.mean(customer_fact['C_ACCTBAL']).alias('mean_acctbal'),
			F.min(customer_fact['C_ACCTBAL']).alias('min_acctbal'),
			F.max(customer_fact['C_ACCTBAL']).alias('max_acctbal')) \
		.select(
				F.col('R_NAME'),
				F.col('N_NAME'),
				F.col('C_MKTSEGMENT'),
				F.col('unique_customers_count'),
				F.col('avg_acctbal'),
				F.col('mean_acctbal'),
				F.col('min_acctbal'),
				F.col('max_acctbal')) \
		.orderBy(F.col('N_NAME').asc(), F.col('C_MKTSEGMENT').asc())

	datamart.show(truncate=False, n=100)
	datamart.write.mode('overwrite').parquet(TARGET_PATH)

def main():
	spark = _spark_session()
	process(spark)
	spark.stop()

if __name__ == '__main__':
	main()
