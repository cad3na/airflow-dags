from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.dates import days_ago

from mx_covid_data import review_csv_files, csv_to_parquet
from mx_covid_data import suspect_time_series, confirmed_time_series, negatives_time_series
from mx_covid_data import suspect_time_series_graph, confirmed_time_series_graph, negatives_time_series_graph

data_url = "http://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/datos_abiertos_covid19.zip"
data_dir = "/home/pi/covid-data/"

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
    'covid-analysis',
    default_args = default_args,
    catchup = False,
    schedule_interval = "5 2 * * *",
)

remove_old_zips = BashOperator(
    task_id = "remove_old_zips",
    bash_command = f'ls {data_dir} | grep -oP ".*.zip" | xargs rm -rf',
    dag = dag,
)

remove_old_dirs = BashOperator(
    task_id = "remove_old_dirs",
    bash_command = f'ls {data_dir} | grep -oP "^\d{6}$" | xargs rm -rf',
    dag = dag,
)

obtain_data = BashOperator(
    task_id = 'obtain_data',
    bash_command = f'curl {data_url} -o {data_dir}covid19-data.zip',
    dag = dag,
)

unzip_data = BashOperator(
    task_id = 'unzip_data',
    bash_command = f'unzip {data_dir}covid19-data.zip -d {data_dir}',
    dag = dag,
)

create_dir = BashOperator(
    task_id = "create_dir",
    bash_command = f'ls -t {data_dir}*COVID19MEXICO.csv ' + '| head -1 |' + 'grep -oP "(\d{6})" ' + f'| mkdir -p "{data_dir}$(cat -)"',
    dag = dag,
)

review_csvs = BranchPythonOperator(
    task_id = "review_csvs",
    python_callable = review_csv_files,
    dag = dag,
)

join = DummyOperator(
    task_id = "join",
    trigger_rule = "none_failed_or_skipped",
    dag = dag,
)

parquet_data = PythonOperator(
    task_id = "parquet_data",
    python_callable = csv_to_parquet,
    dag = dag,
)

suspect_tables = PythonOperator(
    task_id = "suspect_tables",
    python_callable = suspect_time_series,
    dag = dag,
)

confirmed_tables = PythonOperator(
    task_id = "confirmed_tables",
    python_callable = confirmed_time_series,
    dag = dag,
)

negatives_tables = PythonOperator(
    task_id = "negatives_tables",
    python_callable = negatives_time_series,
    dag = dag,
)

suspect_graphs = PythonOperator(
    task_id = "suspect_graphs",
    python_callable = suspect_time_series_graph,
    dag = dag,
)

confirmed_graphs = PythonOperator(
    task_id = "confirmed_graphs",
    python_callable = confirmed_time_series_graph,
    dag = dag,
)

negatives_graphs = PythonOperator(
    task_id = "negatives_graphs",
    python_callable = negatives_time_series_graph,
    dag = dag,
)

remove_old_zips >> review_csvs >> obtain_data >> unzip_data

unzip_data >> join
review_csvs >> join

remove_old_dirs >> parquet_data

join >> create_dir >> parquet_data >> [suspect_tables, confirmed_tables, negatives_tables]

suspect_tables >> suspect_graphs
confirmed_tables >> confirmed_graphs
negatives_tables >> negatives_graphs