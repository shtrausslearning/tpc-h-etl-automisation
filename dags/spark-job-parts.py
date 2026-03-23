import uuid
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType

TARGET_PATH = f's3a://de-project/user/parts_report'

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
	
	part_schema = StructType([
		StructField('P_PARTKEY', LongType(), True),
		StructField('P_NAME', StringType(), True),
		StructField('P_MFGR', StringType(), True),
		StructField('P_BRAND', StringType(), True),
		StructField('P_TYPE', StringType(), True),
		StructField('P_SIZE', LongType(), True),
		StructField('P_CONTAINER', StringType(), True),
		StructField('P_RETAILPRICE', DoubleType(), True),
		StructField('P_COMMENT', StringType(), True),
	])
	
	part_fact = spark.read \
		.schema(part_schema) \
		.parquet(f's3a://de-raw/part')
	
	partsup_schema = StructType([
		StructField('PS_PARTKEY', LongType(), True),
		StructField('PS_SUPPKEY', LongType(), True),
		StructField('PS_AVAILQTY', IntegerType(), True),
		StructField('PS_SUPPLYCOST', DoubleType(), True),
		StructField('PS_COMMENT', StringType(), True),
	])

	partsup_fact = spark.read \
		.schema(partsup_schema) \
		.parquet(f's3a://de-raw/partsupp')
		
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

	datamart = part_fact \
		.join(other=partsup_fact, on=partsup_fact['PS_PARTKEY'] == part_fact['P_PARTKEY'], how='inner') \
		.join(other=supplier_fact, on=supplier_fact['S_SUPPKEY'] == partsup_fact['PS_SUPPKEY'], how='inner') \
		.join(other=nation_fact, on=nation_fact['N_NATIONKEY'] == supplier_fact['S_NATIONKEY'], how='inner') \
		.groupBy(nation_fact['N_NAME'], part_fact['P_TYPE'], part_fact['P_CONTAINER'])  \
		.agg(F.count(part_fact['P_PARTKEY']).alias('parts_count'),
			F.avg(part_fact['P_RETAILPRICE']).alias('avg_retailprice'),
			F.sum(part_fact['P_SIZE']).alias('size'),
			F.mean(part_fact['P_RETAILPRICE']).alias('mean_retailprice'),
			F.min(part_fact['P_RETAILPRICE']).alias('min_retailprice'),
			F.max(part_fact['P_RETAILPRICE']).alias('max_retailprice'),
			F.avg(partsup_fact['PS_SUPPLYCOST']).alias('avg_supplycost'),
			F.mean(partsup_fact['PS_SUPPLYCOST']).alias('mean_supplycost'),
			F.min(partsup_fact['PS_SUPPLYCOST']).alias('min_supplycost'),
			F.max(partsup_fact['PS_SUPPLYCOST']).alias('max_supplycost')) \
		.select(F.col('N_NAME'),
				F.col('P_TYPE'),
				F.col('P_CONTAINER'),
				F.col('parts_count'),
				F.col('avg_retailprice'),
				F.col('size'),
				F.col('mean_retailprice'),
				F.col('min_retailprice'),
				F.col('max_retailprice'),
				F.col('avg_supplycost'),
				F.col('mean_supplycost'),
				F.col('min_supplycost'),
				F.col('max_supplycost')) \
		.orderBy(F.col('N_NAME').asc(), F.col('P_TYPE').asc(), part_fact['P_CONTAINER'].asc())

	datamart.show(truncate=False, n=100)
	datamart.write.mode('overwrite').parquet(TARGET_PATH)

def main():
	spark = _spark_session()
	process(spark)
	spark.stop()

if __name__ == '__main__':
	main()
