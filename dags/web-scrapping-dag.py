# IMPORTS

import json
import requests
import pandas as pd
from requests.adapters import HTTPAdapter
from http.client import responses
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta, date
from airflow.hooks.mysql_hook import MySqlHook

default_args = {
    "owner": "Emili Veiga and Sam Fishwick",
    "depends_on_past": False,
    "start_date": datetime(2022,7, 1),
    "email": ["da@smarttbot.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}



def get_data(**context):

    header = {
        "PRIVATE-TOKEN": "glpat-pyhiN72fzki1sAWgxULC"      
    }

    params = {
    "scope": "all",
    "page": "1", 
    "per_page": "20"
    }

    url = "https://git.smarttbot.com/api/v4/groups/804/issues"

    temp_dict = {'id':[],'iid':[], 'status':[], 'assignees': [], 'state':[], 'BaeisAmazing': [], 'epic': []}

    # Getting Pages Total:
    
    data_head = requests.head(url="https://git.smarttbot.com/api/v4/groups/804/issues", headers=header, params=params)
    dict_response = dict(data_head.headers)["X-Total-Pages"]
    test_page = int(dict_response)
    
    # Inserting data in the temp_dict dictionary:

    for page in range(1,test_page+1):
        params["page"] = page
        response = requests.get(url, params=params, headers=header).json()
        for issue in response:
            temp_dict['id'].append(issue['id'])
            temp_dict['iid'].append(issue['iid'])
            temp_dict['status'].append(issue['labels'])
            temp_dict['state'].append(issue['state'])
            temp_dict['assignees'].append(issue['assignees'])
            temp_dict['BaeisAmazing'].append(issue['milestone'])
            temp_dict['epic'].append(issue['epic'])

    df = pd.DataFrame(data=temp_dict)

    # Cleaning df and renaming it df_str:

    df_str=df.copy()
    df_str["BaeisAmazing"] = df_str["BaeisAmazing"].astype(str)
    df_str["epic"] = df_str["epic"].astype(str)
    df_str["BaeisAmazing"]=df_str["BaeisAmazing"].str.split(":").str[4].str.split("'").str[1]
    df_str["epic"]=df_str["epic"].str.split(":").str[3].str.split("'").str[1]
    df_str["status"] = df_str["status"].astype(str)
    df_str["status"]=df_str["status"].str.split(":").str[2].str.split("'").str[0]
    df_str['status'] = df_str['status'].fillna(df_str['state'])
    df_str


    ti = context["ti"]
    tableName = "gitlab_issues"
    mysql_hook_bi = MySqlHook("mysql_gitlab")
    print("Connecting to DB mysql_gitlab")
    engine_bi = mysql_hook_bi.get_sqlalchemy_engine()
    con = engine_bi.connect()
    with engine_bi.connect() as con_bi:
        print("Sending data to DB mysql_bi")
        df_str.to_sql(name=tableName, con=con_bi, if_exists='replace', index = False)
        print("Data saved successfully")
    
    con.close()

dag = DAG(
    "Gitlab_Metrics",
    default_args=default_args,
    catchup=True,
    schedule_interval='0 11 * * 3',
    max_active_runs=1
)

start_dag = DummyOperator(
    task_id='start_dag',
    dag=dag
)


get_data = PythonOperator(
    task_id="get_data",
    python_callable=get_data,
    provide_context=True,
    dag=dag,
)


end_dag = DummyOperator(
    task_id='end_dag',
    dag=dag
)

start_dag >> get_data >> end_dag