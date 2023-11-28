Build Airflow and Spark image

    Airflow version: 2.7.3

    Spark version: 3.5.0

    Set up the docker file and docker compose properly to run this project.

    run these command.

    mkdir logs | mkdir config | mkdir dags | mkdir plugins | mkdir project_code #create following directories

    docker compose up airflow-init # to build image

    docker compose up -d # run containers

    dokcer compose down # to stop and remove container


Build Scrapy DAG

    cd ./dags

    scrapy startproject Scrapy_Exam_Score_2023_2022

    cd Scrapy_Exam_Score_2023_2022

    scrapy genspider Exam_Score_2022 https://vietnamnet.vn

    scrapy genspider Exam_Score_2023 https://api-university-2022

    #uncomment this line in setting file
    CONCURRENT_REQUESTS = 32

Build Spark App

    add packages = "org.mongodb.spark:mongo-spark-connector_2.12:3.0.2" to connect spark and mongodb

Idea:
    Design a ETL to fetch data National high school graduation exam in 2022 and 2023 on Web and visualize their data.
    Each step uses different tools: 
        Using scrapy to fetch data.
        Using spark to extract-transfer-load data.
        Using mongodb to store final data.
        Using Airflow to coordinate and manage above tasks.
        Using PowerBI to visualize data.
        Using Docker to deloy Airflow and Spark.
    
    Version: 
        Docker - 24.0.6
        Airflow - 2.7.3
        MongoDB - 6.0.7
        Spark - 3.5.0

    PowerBI - https://www.novypro.com/project/vietnamese-national-high-school-graduation-exam-in-2022-and-2023

![Alt text](image.png)