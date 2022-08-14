from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.models.baseoperator import chain, cross_downstream
from datetime import datetime, timedelta  


default_args = {
    'retry': 5,
    "retry_delay": timedelta(minutes = 1)
}

def task_2(my_param, ds, **kwargs):
    print(my_param),
    with open('/tmp/my_file.txt', 'w') as f:
        f.write('my_data')

with DAG(
    dag_id='simple',
    default_args = default_args,
    start_date = datetime(2021, 1, 1),
    schedule_interval = "*/10 * * * *",
    catchup = False,
    max_active_runs = 1
    ) as dag:
        task_1 = DummyOperator(
            task_id = 'task_1'
        )

        task_2 = PythonOperator(
            task_id = 'task_2',
            python_callable = task_2,
            op_kwargs = {'my_param':42}
        )

        task_3 = FileSensor(
            task_id = 'task_3',
            fs_conn_id = 'fs_default', #You gotta create this conn on UI
            filepath = 'my_file.txt'
        )

        task_4 = BashOperator(
            task_id = 'task_4',
            bash_command = 'exit 0'
        )


        task_1 >> task_2 >> task_3 >> task_4
