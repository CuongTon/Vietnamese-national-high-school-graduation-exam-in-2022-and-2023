from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

with DAG("Crawl_Data_2023_and_2022", start_date=datetime(2023, 11, 26), 
         schedule_interval=None, catchup=False
         ) as dag:
    
    start_crawling = EmptyOperator(
        task_id = "start_crawling"
    )

    end_crawling = EmptyOperator(
        task_id = "end_crawling"
    )

    exam_score_2023 = BashOperator(
        task_id = "exam_score_2023",
        bash_command="""
            cd /opt/airflow/project_code/Scrapy_Exam_Score_2023_2022_App/Exam_Score_2023_2022/spiders
            scrapy crawl Exam_Score_2023 -O /opt/airflow/project_code/Scrapy_Exam_Score_2023_2022_App/Data_Final/exam_score_2023.json
        """
    )

    exam_score_2022 = BashOperator(
        task_id = "exam_score_2022",
        bash_command="""
            cd /opt/airflow/project_code/Scrapy_Exam_Score_2023_2022_App/Exam_Score_2023_2022/spiders
            scrapy crawl Exam_Score_2022 -O /opt/airflow/project_code/Scrapy_Exam_Score_2023_2022_App/Data_Final/exam_score_2022.json
        """
    )

    start_crawling >> [exam_score_2023, exam_score_2022] >> end_crawling