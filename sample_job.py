import argparse
import pandas as pd
import logging

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from typing import List
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from smtplib import SMTP


class Pipeline:
    INPUT_FILES = ["/home/hamoor/CS541-Demonstration/data/2018_2019_play_by_play.csv", "/home/hamoor/CS541-Demonstration/data/2019_2020_play_by_play.csv"]
    PORT = 465
    EMAIL = "fakedummy46@gmail.com"
    RECEIVER_EMAIL = "amoor.hari@gmail.com"
    PASSWORD = "tMTFEw8g3krhqNx"

    def Initialize():
        data = list(map(pd.read_csv, Pipeline.INPUT_FILES))
        with open("initialize.pickle", "wb") as fp:
            pickle.dump(data, fp)

    def Aggregate():
        with open("initialize.pickle") as fp:
            data = pd.concat(pickle.load(fp))
        
        with open("aggregate.pickle", "wb") as fp:
            pickle.dump(data.groupby("TEAM").max().to_pickle(), fp)

    def Output():
        with open("aggregate.pickle") as fp:
            result = pickle.load(fp)

        msg = """\
        <html>
          <head></head>
          <body>
            {0}
          </body>
        </html>
        """.format(result.to_html(index=False))

        # Create a secure SSL context
        context = ssl.create_default_context()

        with smtplib.SMTP_SSL("smtp.gmail.com", Pipeline.PORT, context=context) as server:
            server.starttls()
            server.login(Pipeline.EMAIL, Pipeline.PASSWORD)
            server.sendmail(Pipeline.EMAIL, Pipeline.RECEIVER_EMAIL, msg)


dag = DAG(
    "sample_dag",
    default_args={
        'owner': 'Hari Amoor',
        'depends_on_past': False,
        'start_date': days_ago(2),
        'email': ['amoor.hari@gmail.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description="Sample DAG for testing Apache Airflow")

t1 = PythonOperator(
    python_callable=Pipeline.Initialize,
    task_id="Initialize",
    dag=dag)

t2 = PythonOperator(
    python_callable=Pipeline.Aggregate,
    task_id="Aggregate",
    dag=dag)

t3 = PythonOperator(
    python_callable=Pipeline.Output,
    task_id="Output",
    dag=dag)

t1 >> t2 >> t3
