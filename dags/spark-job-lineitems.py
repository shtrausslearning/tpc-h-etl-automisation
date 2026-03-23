import uuid
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType,  LongType

LINEITEM_DATA_PATH = f's3a://de-raw/lineitem'
TARGET_PATH = f's3a://de-project/user/lineitems_report'

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
	
	linetem_schema = StructType([
		StructField('L_ORDERKEY', LongType(), True),
		StructField('L_PARTKEY', IntegerType(), True),
		StructField('L_SUPPKEY', IntegerType(), True),
		StructField('L_LINENUMBER', IntegerType(), True),
		StructField('L_QUANTITY', IntegerType(), True),
		StructField('L_EXTENDEDPRICE', DoubleType(), True),
		StructField('L_DISCOUNT', DoubleType(), True),
		StructField('L_TAX', DoubleType(), True),
		StructField('L_RETURNFLAG', StringType(), True),
		StructField('L_LINESTATUS', StringType(), True),
		StructField('L_SHIPDATE', StringType(), True),
		StructField('L_COMMITDATE', StringType(), True),
		StructField('L_RECEIPTDATE', StringType(), True),
		StructField('L_SHIPINSTRUCT', StringType(), True),
		StructField('L_SHIPMODE', StringType(), True),
		StructField('L_COMMENT', StringType(), True),
	])
	
	linetem_fact = spark.read \
		.schema(linetem_schema) \
		.parquet(LINEITEM_DATA_PATH)

	datamart = linetem_fact \
		.where(linetem_fact['L_ORDERKEY'].isNotNull()) \
		.groupBy(linetem_fact['L_ORDERKEY']) \
		.agg(F.count(linetem_fact['L_ORDERKEY']).alias('count'),
			F.sum(linetem_fact['L_EXTENDEDPRICE']).alias('sum_extendprice'),
			F.mean(linetem_fact['L_DISCOUNT']).alias('mean_discount'),
			F.avg(linetem_fact['L_TAX']).alias('mean_tax'),
			F.avg(F.datediff(linetem_fact['L_RECEIPTDATE'], linetem_fact['L_SHIPDATE'])).alias('delivery_days'),
			F.count(F.when(linetem_fact['L_RETURNFLAG'] == 'A', 1)).alias('A_return_flags'),
			F.count(F.when(linetem_fact['L_RETURNFLAG'] == 'R', 1)).alias('R_return_flags'),
			F.count(F.when(linetem_fact['L_RETURNFLAG'] == 'N', 1)).alias('N_return_flags')) \
		.select(F.col('L_ORDERKEY'),
				F.col('count'),
				F.col('sum_extendprice'),
				F.col('mean_discount'),
				F.col('mean_tax'),
				F.col('delivery_days'),
				F.col('A_return_flags'),
				F.col('R_return_flags'),
				F.col('N_return_flags')) \
		.orderBy(F.col('L_ORDERKEY').asc())

	datamart.show(truncate=False, n=100)
	datamart.write.mode('overwrite').parquet(TARGET_PATH)

def main():
	spark = _spark_session()
	process(spark)
	spark.stop()


if __name__ == '__main__':
	main()
