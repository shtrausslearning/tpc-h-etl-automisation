import uuid
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

TARGET_PATH = f's3a://de-project/user/suppliers_report'

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
	
	supplier_schema = StructType([
		StructField('S_SUPPKEY', LongType(), True),
		StructField('S_NAME', StringType(), True),
		StructField('S_ADDRESS', StringType(), True),
		StructField('S_NATIONKEY', LongType(), True),
		StructField('S_PHONE', StringType(), True),
		StructField('S_ACCTBAL', DoubleType(), True),
		StructField('S_COMMENT', StringType(), True),
	])
	
	supplier_fact = spark.read \
		.schema(supplier_schema) \
		.parquet(f's3a://de-raw/supplier')
		
	nation_schema = StructType([
		StructField('N_NATIONKEY', LongType(), True),
		StructField('N_NAME', StringType(), True),
		StructField('N_REGIONKEY', LongType(), True),
		StructField('N_COMMENT', StringType(), True),
	])

	nation_fact = spark.read \
		.schema(nation_schema) \
		.parquet(f's3a://de-raw/nation')
		
	region_schema = StructType([
		StructField('R_REGIONKEY', LongType(), True),
		StructField('R_NAME', StringType(), True),
		StructField('R_COMMENT', StringType(), True),
	])
	
	region_fact = spark.read \
		.schema(region_schema) \
		.parquet(f's3a://de-raw/region')

	datamart = supplier_fact \
		.join(other=nation_fact, on=nation_fact['N_NATIONKEY'] == supplier_fact['S_NATIONKEY'], how='inner') \
		.join(other=region_fact, on=region_fact['R_REGIONKEY'] == nation_fact['N_REGIONKEY'], how='inner') \
		.groupBy(nation_fact['N_NAME'], region_fact['R_NAME']) \
		.agg(F.count(supplier_fact['S_SUPPKEY']).alias('unique_supplers_count'),
			F.avg(supplier_fact['S_ACCTBAL']).alias('avg_acctbal'),
			F.mean(supplier_fact['S_ACCTBAL']).alias('mean_acctbal'),
			F.min(supplier_fact['S_ACCTBAL']).alias('min_acctbal'),
			F.max(supplier_fact['S_ACCTBAL']).alias('max_acctbal')) \
		.select(F.col('R_NAME'),
				F.col('N_NAME'),
				F.col('unique_supplers_count'),
				F.col('avg_acctbal'),
				F.col('mean_acctbal'),
				F.col('min_acctbal'),
				F.col('max_acctbal')) \
		.orderBy(F.col('R_NAME').asc(), F.col('N_NAME').asc())

	datamart.show(truncate=False, n=100)
	datamart.write.mode('overwrite').parquet(TARGET_PATH)

def main():
	spark = _spark_session()
	process(spark)
	spark.stop()

if __name__ == '__main__':
	main()
