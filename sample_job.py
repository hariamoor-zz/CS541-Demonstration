import argparse
import pandas as pd
import logging

from airflow import DAG
from airflow.operators import python_operator
from pandasql import sqldf
from typing import List
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from smtplib import SMTP


class Pipeline:
    INPUT_FILES = ["data/2018_2019_play_by_play.csv", "data/2019_2020_play_by_play.csv"]

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

        return """\
        <html>
          <head></head>
          <body>
            {0}
          </body>
        </html>
        """.format(result.to_html(index=False))

dag = DAG("sample_dag")
t1 = PythonOperator(
    task_id="Initialize"
    python_callable=Pipeline.Initialize,
    dag=dag)

t2 = PythonOperator(
    task_id="Aggregate",
    python_callable=Pipeline.Aggregate,
    dag=dag)

t3 = EmailOperator(
    task_id="Output",
    to="amoor.hari@gmail.com",
    subject="CS541 Test Email",
    html_content=Pipeline.Output(),
    dag=dag)

t1 >> t2 >> t3