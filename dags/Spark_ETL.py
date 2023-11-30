from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
import pymongo

def _branch():
    try:
        board_of_examiners = open(r'/opt/airflow/project_code/Scrapy_Exam_Score_2023_2022_App/Data_Final/board_of_examiners.csv', 'r')
        exam_score_2022 = open(r'/opt/airflow/project_code/Scrapy_Exam_Score_2023_2022_App/Data_Final/exam_score_2022.json', 'r')
        exam_score_2023 = open(r'/opt/airflow/project_code/Scrapy_Exam_Score_2023_2022_App/Data_Final/exam_score_2023.json', 'r')
        return 'Main_ETL'
    except:
        return 'end_ETL'


with DAG("Spark_ETL", 
         schedule_interval=None, 
         catchup=False, 
         start_date=datetime(2023, 11, 26)) as dag:
    
    #Branch and Empty task
    start_ETL = EmptyOperator(
        task_id = "start_ETL"
    )

    end_ETL = EmptyOperator(
        task_id = "end_ETL",
        trigger_rule="none_failed"
    )

    check_file_existence = BranchPythonOperator(
        task_id = "check_file_existence",
        python_callable=_branch
    )

    # Main task processing
    Main_ETL = EmptyOperator(
        task_id = "Main_ETL"
    )

    Process_Exam_Score_2023 = SparkSubmitOperator(
        task_id = "Process_Exam_Score_2023",
        conn_id="spark_local_host",
        application = "/opt/airflow/project_code/Spark_App/Exam_Score_ETL_2023.py",
        packages = "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2"
    )

    Process_Exam_Score_2022 = SparkSubmitOperator(
        task_id = "Process_Exam_Score_2022",
        conn_id="spark_local_host",
        application = "/opt/airflow/project_code/Spark_App/Exam_Score_ETL_2022.py",
        packages = "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2"
    )

    Process_Board_Of_Examiners = SparkSubmitOperator(
        task_id = "Process_Board_Of_Examiners",
        conn_id="spark_local_host",
        application = "/opt/airflow/project_code/Spark_App/Board_of_Examiner_ETL.py",
        packages = "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2"
    )

    start_ETL >> check_file_existence >> [Main_ETL, end_ETL]
    Main_ETL >> [Process_Exam_Score_2023, Process_Exam_Score_2022, Process_Board_Of_Examiners] >> end_ETL