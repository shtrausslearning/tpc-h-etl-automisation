import uuid
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, LongType, FloatType

TARGET_PATH = f's3a://de-project/user/orders_report'

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
	
	orders_schema = StructType([
		StructField('O_ORDERKEY', LongType(), True),
		StructField('O_CUSTKEY', LongType(), True),
		StructField('O_ORDERSTATUS', StringType(), True),
		StructField('O_TOTALPRICE', DoubleType(), True),
		StructField('O_ORDERDATE', StringType(), True),
		StructField('O_ORDERPRIORITY', StringType(), True),
		StructField('O_CLERK', StringType(), True),
		StructField('O_SHIPPRIORITY', IntegerType(), True),
		StructField('O_COMMENT', StringType(), True),
	])
	
	orders_fact = spark.read \
		.schema(orders_schema) \
		.parquet(f's3a://de-raw/orders')
		
	customer_schema = StructType([
		StructField('C_CUSTKEY', IntegerType(), True),
		StructField('C_NAME', StringType(), True),
		StructField('C_ADDRESS', StringType(), True),
		StructField('C_NATIONKEY', IntegerType(), True),
		StructField('C_PHONE', StringType(), True),
		StructField('C_ACCTBAL', FloatType(), True),
		StructField('C_MKTSEGMENT', StringType(), True),
		StructField('C_COMMENT', StringType(), True),
	])
	
	customer_fact = spark.read \
		.schema(customer_schema) \
		.parquet(f's3a://de-raw/customer')
		
	nation_schema = StructType([
		StructField('N_NATIONKEY', LongType(), True),
		StructField('N_NAME', StringType(), True),
		StructField('N_REGIONKEY', IntegerType(), True),
		StructField('N_COMMENT', StringType(), True),
	])

	nation_fact = spark.read \
		.schema(nation_schema) \
		.parquet(f's3a://de-raw/nation')

	datamart = orders_fact \
		.join(other=customer_fact, on=customer_fact['C_CUSTKEY'] == orders_fact['O_CUSTKEY'], how='left') \
		.join(other=nation_fact, on=nation_fact['N_NATIONKEY'] == customer_fact['C_NATIONKEY'], how='left') \
		.groupBy(F.date_format(orders_fact['O_ORDERDATE'], 'yyyy-MM').alias('O_MONTH'), nation_fact['N_NAME'], orders_fact['O_ORDERPRIORITY']) \
		.agg(F.count(orders_fact['O_ORDERKEY']).alias('orders_count'),
			F.avg(orders_fact['O_TOTALPRICE']).alias('avg_order_price'),
			F.sum(orders_fact['O_TOTALPRICE']).alias('sum_order_price'),
			F.min(orders_fact['O_TOTALPRICE']).alias('min_order_price'),
			F.max(orders_fact['O_TOTALPRICE']).alias('max_order_price'),
			F.count(F.when(orders_fact['O_ORDERSTATUS'] == 'F', 1)).alias('f_order_status'),
			F.count(F.when(orders_fact['O_ORDERSTATUS'] == 'O', 1)).alias('o_order_status'),
			F.count(F.when(orders_fact['O_ORDERSTATUS'] == 'P', 1)).alias('p_order_status')) \
		.select(F.col('O_MONTH'),
				F.col('N_NAME'),
				F.col('O_ORDERPRIORITY'),
				F.col('orders_count'),
				F.col('avg_order_price'),
				F.col('sum_order_price'),
				F.col('min_order_price'),
				F.col('max_order_price'),
				F.col('f_order_status'),
				F.col('o_order_status'),
				F.col('p_order_status')) \
		.orderBy(F.col('N_NAME').asc(), F.col('O_ORDERPRIORITY').asc())

	datamart.show(truncate=False, n=100)
	datamart.write.mode('overwrite').parquet(TARGET_PATH)

def main():
	spark = _spark_session()
	process(spark)
	spark.stop()

if __name__ == '__main__':
	main()
