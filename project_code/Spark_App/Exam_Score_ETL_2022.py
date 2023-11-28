from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .master("local") \
    .appName("Exam_Score_ETL_2022") \
    .getOrCreate()

# import file exam_score from mongodb & board_of_examiner from local disk

exam_score_2022_raw = spark.read \
    .format("json") \
    .option("multiline", "true") \
    .load("/opt/airflow/project_code/Scrapy_Exam_Score_2023_2022_App/Data_Final/exam_score_2022.json")

# remove redundant column & rename column & add new province column

adjust_column_exam_score_2022 = exam_score_2022_raw \
    .selectExpr("id",
                "`Toán` as mathematics",
                "`Lí` as physics",
                "`Hóa` as chemistry",
                "`Ngoại ngữ` as foreign_language",
                "`Văn` as literature",
                "`Sử` as history",
                "`Địa` as geography",
                "Sinh as biology",
                "GDCD as civic_education"
                ) \
    .withColumn("board_id", expr("left(id, length(id)-6)").cast(IntegerType())) \
    .sort(col("id").asc())

#save file to MongoDB for final use

adjust_column_exam_score_2022.write \
    .format("com.mongodb.spark.sql.DefaultSource") \
    .option('uri', 'mongodb://host.docker.internal/national_exam_score_ready_to_use.exam_score_2022') \
    .mode("overwrite") \
    .save()
