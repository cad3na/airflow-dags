from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'roberto',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['roberto@cad3na.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dag-update',
    default_args = default_args,
    catchup = False,
    schedule_interval = "* */15 * * *",
)

git_pull = BashOperator(
    task_id = "update_dags",
    bash_command = f'cd /home/pi/airflow/dags && git pull',
    dag = dag,
)