from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

def csv_to_parquet():
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pathlib as pl
    
    import os
    import re

    dtypes = {
        'ID_REGISTRO':'object',
        'ORIGEN':int,
        'SECTOR':int,
        'ENTIDAD_UM':int,
        'SEXO':int,
        'ENTIDAD_NAC':int,
        'ENTIDAD_RES':int,
        'MUNICIPIO_RES':int,
        'TIPO_PACIENTE':int,
        'INTUBADO':int,
        'NEUMONIA':int,
        'EDAD':int,
        'NACIONALIDAD':int,
        'EMBARAZO':int,
        'HABLA_LENGUA_INDIG':int,
        'INDIGENA':int,
        'DIABETES':int,
        'EPOC':int,
        'ASMA':int,
        'INMUSUPR':int,
        'HIPERTENSION':int,
        'OTRA_COM':int,
        'CARDIOVASCULAR':int,
        'OBESIDAD':int,
        'RENAL_CRONICA':int,
        'TABAQUISMO':int,
        'OTRO_CASO':int,
        'TOMA_MUESTRA_LAB':int,
        'RESULTADO_LAB':int,
        'TOMA_MUESTRA_ANTIGENO':int,
        'RESULTADO_ANTIGENO':int,
        'CLASIFICACION_FINAL':int,
        'MIGRANTE':int,
        'PAIS_NACIONALIDAD':'object',
        'PAIS_ORIGEN':'object',
        'UCI':int
    }

    date_cols = ["FECHA_ACTUALIZACION", "FECHA_INGRESO", "FECHA_SINTOMAS", "FECHA_DEF"]

    data_dir = pl.Path("/home/pi/covid-data/")

    csv_list = list(data_dir.glob("*COVID19MEXICO.csv"))
    csv_list.sort(key=os.path.getctime, reverse=True)

    csv_file = csv_list[0]
    csv_date = re.findall("(\d{6})COVID19MEXICO.csv", csv_file)[0]

    parquet_dir = data_dir/f"{csv_date}.parquet"

    chunksize = 100_000

    csv_stream = pd.read_csv(str(csv_file),
                             dtype=dtypes,
                             parse_dates=date_cols,
                             encoding="latin-1",
                             chunksize=chunksize)

    metadata_collector = []
    for i, chunk in enumerate(csv_stream):
        print("Chunk", i)

        table = pa.Table.from_pandas(df=chunk)
        
        pq.write_to_dataset(table,
                            root_path=str(parquet_dir),
                            partition_cols=["ENTIDAD_UM"],
                            metadata_collector=metadata_collector)

    pq.write_metadata(table.schema, str(parquet_dir/"_common_metadata"))

def suspect_time_series():
    from os import listdir
    from os.path import getctime
    from re import search, findall
    from copy import copy
    from pandas import read_pickle
    from glob import glob

    direcs = listdir("/home/pi/covid-data")
    lista = ["/home/pi/covid-data/" + file for file in direcs if search("^\d{6}$", file)]
    lista.sort(key=getctime, reverse=True)
    direc = lista[0]
    pkl_files = glob(direc + "/*.pkl" )
    pkl_files.sort(key=lambda x: int(x[-7:-4]))
    fecha = findall("(\d{6})", direc)[0]

    df = read_pickle(pkl_files[0])
    index = df['CLASIFICACION_FINAL'] == 6
    sos = df[index].groupby(['ENTIDAD_UM', 'FECHA_INGRESO']).count()['ORIGEN']
    result = copy(sos)
    for file in pkl_files[1:]:
        df = read_pickle(file)
        index = df['CLASIFICACION_FINAL'] == 6
        sos = df[index].groupby(['ENTIDAD_UM', 'FECHA_INGRESO']).count()['ORIGEN']
        result.add(sos, fill_value=0)

    result.to_csv(f"{direc}/sospechosos_{fecha}.csv")

def confirmed_time_series():
    from os import listdir
    from os.path import getctime
    from re import search, findall
    from copy import copy
    from pandas import read_pickle
    from glob import glob

    direcs = listdir("/home/pi/covid-data")
    lista = ["/home/pi/covid-data/" + file for file in direcs if search("^\d{6}$", file)]
    lista.sort(key=getctime, reverse=True)
    direc = lista[0]
    pkl_files = glob(direc + "/*.pkl" )
    pkl_files.sort(key=lambda x: int(x[-7:-4]))
    fecha = findall("(\d{6})", direc)[0]

    df = read_pickle(pkl_files[0])
    index = df['CLASIFICACION_FINAL'].isin([1, 2, 3])
    con = df[index].groupby(['ENTIDAD_UM', 'FECHA_INGRESO']).count()['ORIGEN']
    result = copy(con)
    for file in pkl_files[1:]:
        df = read_pickle(file)
        index = df['CLASIFICACION_FINAL'].isin([1, 2, 3])
        con = df[index].groupby(['ENTIDAD_UM', 'FECHA_INGRESO']).count()
        result.add(con, fill_value=0)

    result.to_csv(f"{direc}/confirmados_{fecha}.csv")

def negatives_time_series():
    from os import listdir
    from os.path import getctime
    from re import search, findall
    from copy import copy
    from pandas import read_pickle
    from glob import glob

    direcs = listdir("/home/pi/covid-data")
    lista = ["/home/pi/covid-data/" + file for file in direcs if search("^\d{6}$", file)]
    lista.sort(key=getctime, reverse=True)
    direc = lista[0]
    pkl_files = glob(direc + "/*.pkl" )
    pkl_files.sort(key=lambda x: int(x[-7:-4]))
    fecha = findall("(\d{6})", direc)[0]

    df = read_pickle(pkl_files[0])
    index = df['CLASIFICACION_FINAL'] == 7
    neg = df[index].groupby(['ENTIDAD_UM', 'FECHA_INGRESO']).count()['ORIGEN']
    result = copy(neg)
    for file in pkl_files[1:]:
        df = read_pickle(file)
        index = df['CLASIFICACION_FINAL'] == 7
        neg = df[index].groupby(['ENTIDAD_UM', 'FECHA_INGRESO']).count()['ORIGEN']
        result.add(neg, fill_value=0)

    result.to_csv(f"{direc}/negativos_{fecha}.csv")

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
    schedule_interval = None,
)

#remove_old_csvs = BashOperator(
#    task_id = "remove_old_csvs",
#    bash_command = 'ls /home/pi/covid-data | grep -oP ".*.csv" | xargs rm -rf',
#    dag = dag,
#)

remove_old_zips = BashOperator(
    task_id = "remove_old_zips",
    bash_command = 'ls /home/pi/covid-data | grep -oP ".*.zip" | xargs rm -rf',
    dag = dag,
)

remove_old_dirs = BashOperator(
    task_id = "remove_old_dirs",
    bash_command = 'ls /home/pi/covid-data | grep -oP "^\d{6}$" | xargs rm -rf',
    dag = dag,
)

obtain_data = BashOperator(
    task_id = 'obtain_data',
    bash_command = 'curl http://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/datos_abiertos_covid19.zip -o /home/pi/covid-data/covid19-data.zip',
    dag = dag,
)

unzip_data = BashOperator(
    task_id = 'unzip_data',
    bash_command = 'unzip /home/pi/covid-data/covid19-data.zip -d /home/pi/covid-data/',
    dag = dag,
)

create_dir = BashOperator(
    task_id = "create_dir",
    bash_command = 'ls -t /home/pi/covid-data/*COVID19MEXICO.csv | head -1 | grep -oP "(\d{6})" | mkdir -p "/home/pi/covid-data/$(cat -)"' ,
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

#remove_old_csvs >> unzip_data
remove_old_zips >> obtain_data
remove_old_dirs >> create_dir

obtain_data >> unzip_data >> create_dir  >> parquet_data

parquet_data >> suspect_tables >> confirmed_tables >> negatives_tables
