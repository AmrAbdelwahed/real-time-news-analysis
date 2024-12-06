import os
import time
from newsapi import NewsApiClient
from kafka import KafkaProducer
import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Step 1: Fetch data from NewsAPI
def fetch_news_articles(api_key, keywords):
    newsapi = NewsApiClient(api_key=api_key)
    articles = newsapi.get_everything(q=keywords,
                                    language='en',
                                    sort_by='relevancy')

    print("\nPreview of first 5 articles:")

    for i, article in enumerate(articles['articles'][:5], 1):
        print(f"\nArticle {i}:")
        print(f"Title: {article['title']}")
        print(f"Published at: {article['publishedAt']}")
        print(f"URL: {article['url']}")
        print("-" * 80)  # Separator line

    return articles['articles']

# Step 2: Set up Kafka Producer
def produce_to_kafka(articles, topic_name, kafka_bootstrap_servers):
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    #error handling and confirmation
    success_count = 0
    for i, article in enumerate(articles):
        if article.get('title') == '[Removed]':  # Skip removed articles
            continue
        try:
            future = producer.send(topic_name, value=article)
            record_metadata = future.get(timeout=10)  # Wait for confirmation
            success_count += 1

             # first 5 articles being successfully sent to sent to Kafka
            if i < 5:
                print(f"\nSending Article {i+1}:")
                print(f"Title: {article['title']}")
                print(f"To partition: {record_metadata.partition}")
                print(f"At offset: {record_metadata.offset}")
                print("-" * 80)

        except Exception as e:
            print(f"Error sending article to Kafka: {str(e)}")
    
    producer.flush()
    print(f"Successfully sent {success_count} articles to Kafka")

# Step 3: Set up Spark to read from Kafka
def consume_from_kafka(topic_name, kafka_bootstrap_servers, warehouse_path):
    # Initialize Spark with Kafka packages
    spark = (SparkSession.builder
             .appName("NewsArticleAnaalysis")
             .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1")
             .config("spark.sql.warehouse.dir", warehouse_path)
             .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint")
             .enableHiveSupport()
             .getOrCreate())

    # Define schema for the news articles
    schema = StructType([
        StructField("title", StringType(), True),
        StructField("description", StringType(), True),
        StructField("url", StringType(), True),
        StructField("published_at", TimestampType(), True)
    ])

    checkpoint_path = "/tmp/checkpoint"
    table_path = os.path.join(warehouse_path, "news_articles")

    # Clear previous data and checkpoint
    spark.sql("DROP TABLE IF EXISTS news_articles")
    try:
        # Remove checkpoint directory
        os.system(f"hadoop fs -rm -r {checkpoint_path}")
        # Remove table directory
        os.system(f"hadoop fs -rm -r {table_path}")
    except:
        pass

    # Read from Kafka
    df = (spark
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
          .option("subscribe", topic_name)
          .option("startingOffsets", "earliest")
          .load())

    # Parse JSON value and select fields
    parsed_df = df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    # Write to Hive warehouse directory in Parquet format

    query = (parsed_df
             .writeStream
             .outputMode("append")
             .format("parquet")
             .option("path", table_path)
             .start())

    # Wait for some data to be written
    query.awaitTermination(60)  # Wait for 60 seconds
    query.stop()

    # Register the table in Hive    
    spark.sql(f"""
        CREATE EXTERNAL TABLE news_articles (
            title STRING,
            description STRING,
            url STRING,
            published_at TIMESTAMP
        )
        STORED AS PARQUET
        LOCATION '{table_path}'
    """)

    return spark

def analyze_data_with_hive(spark):
    # Verify data exists
    count = spark.sql("SELECT COUNT(*) as count FROM news_articles").collect()[0]['count']
    print(f"Total articles in table: {count}")
    
    if count > 0:
        # Example analytics queries
        print("\nTop 10 articles by description length:")
        spark.sql("""
            SELECT title, length(description) as word_count
            FROM news_articles
            WHERE description IS NOT NULL
            ORDER BY word_count DESC
            LIMIT 10
        """).show(truncate=False)
    else:
        print("No data found in the news_articles table")


if __name__ == "__main__":
    # Set up parameters
    api_key = "1a5874116d49471f82ec33c9c48422b6"
    kafka_bootstrap_servers = "project1-m:9092"
    kafka_topic = "news-articles"
    warehouse_path = "/user/hive/warehouse"
    
    # Execute pipeline
    try:
        # Fetch articles and produce to Kafka
        print("Fetching articles from NewsAPI...")
        articles = fetch_news_articles(api_key, "technology")
        print(f"Retrieved {len(articles)} articles")

        print("\nSending articles to Kafka...")
        produce_to_kafka(articles, kafka_topic, kafka_bootstrap_servers)

        print("\nProcessing data with Spark...")
        spark = consume_from_kafka(kafka_topic, kafka_bootstrap_servers, warehouse_path)

        print("\nAnalyzing data...")
        analyze_data_with_hive(spark)

    except Exception as e:
        print(f"Error in pipeline: {str(e)}")
    finally:
        spark.stop()
