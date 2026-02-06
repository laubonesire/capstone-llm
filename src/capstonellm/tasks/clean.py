import argparse
import logging
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from capstonellm.common.catalog import llm_bucket
from capstonellm.common.spark import ClosableSparkSession
from dotenv import load_dotenv
import os

load_dotenv()  # Loads the `.env` file
aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

logger = logging.getLogger(__name__)

def clean(spark: SparkSession, environment: str, tag: str):
    questions = spark.read.json(f"s3a://{llm_bucket}/input/{tag}/questions.json")

    df = questions.withColumn("question", f.explode("items"))
    df = df.select("question.*")
    df = df.select("question_id", "body", "title", "link").withColumnRenamed(
        "body", "question"
    )
    df.show()

    answers = spark.read.json(f"s3a://{llm_bucket}/input/{tag}/answers.json")

    dfa = answers.withColumn("answer", f.explode("items"))
    dfa = dfa.select("answer.*")
    dfa = dfa.select("question_id", "answer_id", "body").withColumnRenamed(
        "body", "answer"
    )

    joined = df.join(dfa, "question_id")
    joined = joined.withColumn("question_id", f.col("question_id").cast("string"))
    joined = joined.withColumn("answer_id", f.col("answer_id").cast("string"))

    count = joined.count()
    joined.repartition(count).write.mode("overwrite").json(
        f"s3a://{llm_bucket}/cleaned/{tag}/"
    )

    # Writing joined dataframe to file
    # DO NOT RUN joined_df.write.mode("overwrite").json("./")

def main():
    parser = argparse.ArgumentParser(description="capstone_llm")
    parser.add_argument(
        "-e", "--env", dest="env", help="environment we are executing in", required=False, default="local"
    )
    parser.add_argument(
        "-t", "--tag", dest="tag", help="the tag to process",
        default="python-polars", required=False
    )
    logger.info("starting the cleaning job")

    args = parser.parse_args()
    common_spark_config = {
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
    }
    if args.env == "local":
        print("This is a local execution of the capestonellm project")
        session = (
            SparkSession.builder.appName("Spark S3 Integration")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
            .getOrCreate()
        )
        clean(session, args.env, args.tag)
    else:
        with ClosableSparkSession("capstone_llm", spark_config=common_spark_config) as session:
            clean(session, args.env, args.tag)


if __name__ == "__main__":
    main()

