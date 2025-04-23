from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, explode, from_json, concat_ws
from pyspark.sql.types import ArrayType, StringType, StructType
import spacy

# spaCy Model
nlp = spacy.load("en_core_web_sm")

# Spark Session
spark = SparkSession.builder \
    .appName("NERWithFinnhub") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Extract Name Entities
def extract_named_entities(text):
    doc = nlp(text)
    return [ent.text for ent in doc.ents]

extract_entities_udf = udf(extract_named_entities, ArrayType(StringType()))

# Kafka Input Schema
schema = StructType() \
    .add("headline", StringType()) \
    .add("summary", StringType()) \
    .add("datetime", StringType())

# Kafka topic1
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "topic1") \
    .option("startingOffsets", "latest") \
    .load()

# Parse JSON Value
parsed_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema).alias("data")) \
    .select("data.*")

# Name Entity Recognition
text_df = parsed_df.withColumn("combined_text", concat_ws(" ", col("headline"), col("summary")))

# Extract NER
entities_df = text_df.withColumn("entities", extract_entities_udf("combined_text")) \
    .select(explode("entities").alias("entity"))

# Count NER
entity_count_df = entities_df.groupBy("entity").count()

# Display NER (for check)
entity_count_df.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Write into Kafka topic2
output_df = entity_count_df.selectExpr("to_json(struct(*)) AS value")

query = output_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("topic", "topic2") \
    .option("checkpointLocation", "/tmp/spark_checkpoint_finnhub") \
    .outputMode("complete") \
    .start()

query.awaitTermination()
