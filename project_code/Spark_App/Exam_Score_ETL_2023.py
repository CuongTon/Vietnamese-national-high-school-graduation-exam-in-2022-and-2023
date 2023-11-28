from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


spark = SparkSession.builder \
    .master("local") \
    .appName("Exam_Score_ETL_2023") \
    .getOrCreate()

# import file exam_score from mongodb & board_of_examiner from local disk
exam_score_2023_raw = spark.read \
    .format("json") \
    .option("multiline", "true") \
    .load("/opt/airflow/project_code/Scrapy_Exam_Score_2023_2022_App/Data_Final/exam_score_2023.json")


# remove redundant column & rename column & add new province column

adjust_column_exam_score_2023 = exam_score_2023_raw \
    .selectExpr("id",
                "mathematics_score as mathematics",
                "chemistry_score as chemistry",
                "physics_score as physics",
                "foreign_language_score as foreign_language",
                "literature_score as literature",
                "history_score as history",
                "geography_score as geography",
                "biology_score as biology",
                "civic_education_score as civic_education"
                ) \
    .withColumn("board_id", expr("left(id, length(id)-6)").cast(IntegerType())) \
    .sort(col("id").asc())


#save file to MongoDB for final use

adjust_column_exam_score_2023.write \
    .format("com.mongodb.spark.sql.DefaultSource") \
    .option('uri', 'mongodb://host.docker.internal/national_exam_score_ready_to_use.exam_score_2023') \
    .mode("overwrite") \
    .save()
