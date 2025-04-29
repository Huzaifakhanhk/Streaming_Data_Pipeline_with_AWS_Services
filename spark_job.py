
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

def run_spark_job(config):
    kafka_config = config['kafka']
    db_config = config['database']

    spark = SparkSession.builder.appName("KafkaSparkRDSJob").getOrCreate()
    
    df = spark.read.format("kafka").option("kafka.bootstrap.servers", kafka_config['bootstrap_servers']).option("subscribe", kafka_config['topic']).option("startingOffsets", "earliest").load()
    
    schema = StructType([StructField("id", IntegerType()), StructField("value", IntegerType()), StructField("description", StringType())])
    
    json_df = df.select(col("value").cast("string").alias("json"))
    parsed_df = json_df.select(from_json(col("json"), schema).alias("data")).select("data.*")
    
    result_df = parsed_df.withColumn("processed_value", col("value") * 2)

    url = f"jdbc:postgresql://{db_config['host']}:{db_config['port']}/{db_config['name']}"
    result_df.write.format("jdbc").option("url", url).option("dbtable", "processed_data").option("user", db_config['user']).option("password", db_config['password']).mode("append").save()
    
    spark.stop()
