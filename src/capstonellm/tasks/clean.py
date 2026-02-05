import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from capstonellm.common.catalog import llm_bucket
from capstonellm.common.spark import ClosableSparkSession
from dotenv import load_dotenv
import os

load_dotenv()  # Loads the `.env` file
aws_access_key = os.getenv("AWS_ACCESS_KEY_ID")
aws_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")

logger = logging.getLogger(__name__)

def clean(spark: SparkSession, environment: str, tag: str):
    #print("Start of cleaning job!")

    # Loading questions.json into dataframe
    questions_df = spark.read.json("s3a://dataminded-academy-capstone-llm-data-us/input/dbt/questions.json")

    # Explode the 'items' array to create a row for each struct in the array
    exploded_questions_df = questions_df.select(explode("items").alias("item"))

    # Select the desired columns from the exploded df
    filtered_questions_df = exploded_questions_df.select("item.question_id", "item.title", "item.body")

    # Giving specific name to columns to avoid duplicates post join
    filtered_questions_renamed_df = filtered_questions_df.withColumnsRenamed(
        {"title": "question_title", "body": "question_body"}
        )

    filtered_questions_renamed_df.show()
    #print("End of cleaning job!")

    # Loading answerrs.json into dataframe
    answers_df = spark.read.json("s3a://dataminded-academy-capstone-llm-data-us/input/dbt/answers.json")

    # Explode the 'items' array to create a row for each struct in the array
    exploded_answers_df = answers_df.select(explode("items").alias("item"))

    # Select the desired columns from the exploded df
    filtered_answers_df = exploded_answers_df.select("item.answer_id", "item.body", "item.question_id", "item.is_accepted")

    # Giving specific name to columns to avoid duplicates post join
    filtered_answers_renamed_df = filtered_answers_df.withColumnsRenamed(
        {"body": "answer_body", "is_accepted": "answer_is_accepted"}
        )

    filtered_answers_renamed_df.show()

    # Joining the filtered df togethers on question_id
    joined_df = filtered_questions_renamed_df.join(
        filtered_answers_renamed_df,
        on="question_id",
        how="outer"
    )

    joined_df.show()

    # Writing joined dataframe to file
    # DO NOT RUN joined_df.write.mode("overwrite").json("./")
    # RUN THIS INSTEAD 
    joined_df.write.mode("overwrite").json("s3a://dataminded-academy-capstone-llm-data-us/output/dbt/")


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

