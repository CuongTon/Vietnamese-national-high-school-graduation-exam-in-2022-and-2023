from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

import re

def no_accent_vietnamese(s):
    s = re.sub(r'[àáạảãâầấậẩẫăằắặẳẵ]', 'a', s)
    s = re.sub(r'[ÀÁẠẢÃĂẰẮẶẲẴÂẦẤẬẨẪ]', 'A', s)
    s = re.sub(r'[èéẹẻẽêềếệểễ]', 'e', s)
    s = re.sub(r'[ÈÉẸẺẼÊỀẾỆỂỄ]', 'E', s)
    s = re.sub(r'[òóọỏõôồốộổỗơờớợởỡ]', 'o', s)
    s = re.sub(r'[ÒÓỌỎÕÔỒỐỘỔỖƠỜỚỢỞỠ]', 'O', s)
    s = re.sub(r'[ìíịỉĩ]', 'i', s)
    s = re.sub(r'[ÌÍỊỈĨ]', 'I', s)
    s = re.sub(r'[ùúụủũưừứựửữ]', 'u', s)
    s = re.sub(r'[ƯỪỨỰỬỮÙÚỤỦŨ]', 'U', s)
    s = re.sub(r'[ỳýỵỷỹ]', 'y', s)
    s = re.sub(r'[ỲÝỴỶỸ]', 'Y', s)
    s = re.sub(r'[Đ]', 'D', s)
    s = re.sub(r'[đ]', 'd', s)

    marks_list = [u'\u0300', u'\u0301', u'\u0302', u'\u0303', u'\u0306',u'\u0309', u'\u0323']

    for mark in marks_list:
        s = s.replace(mark, '')

    return s

spark = SparkSession.builder \
    .master("local") \
    .appName("Board_of_Examiner_ETL") \
    .getOrCreate()

# import file exam_score from mongodb & board_of_examiner from local disk

board_of_examiners_raw = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load("/opt/airflow/project_code/Scrapy_Exam_Score_2023_2022_App/Data_Final/board_of_examiners.csv")

# modify board_of_examiners_raw: change to no vietnamese accent (Sở GDĐT Hà Nội > So GDDT Ha Noi) & modify value of column (So GDDT Ha Noi > Ha Noi)

udf_no_accent_vietnamese = udf(no_accent_vietnamese, StringType())

adjust_board_of_examiners = board_of_examiners_raw \
    .selectExpr("`Mã Hội đồng thi` as board_id", "`Tên Hội đồng thi` as province_accent_vietnamese") \
    .withColumn("province", udf_no_accent_vietnamese("province_accent_vietnamese")) \
    .drop("province_accent_vietnamese") \
    .withColumn("province", expr("substring(province, 8, length(province) - 7)")) \
    .withColumn("board_id", col("board_id").cast(IntegerType()))

#save file to MongoDB for final use

adjust_board_of_examiners.write \
    .format("com.mongodb.spark.sql.DefaultSource") \
    .option('uri', 'mongodb://host.docker.internal/national_exam_score_ready_to_use.board_of_examiners') \
    .mode("overwrite") \
    .save()