
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

def review_csv_files():
    from datetime import datetime, timedelta
    import pathlib as pl

    data_dir = pl.Path("/home/pi/covid-data/")

    date = datetime.now()
    if date.hour < 21:
        date = date - timedelta(days=1)
    
    date = date.strftime("%y%m%d")
    csvs = list(data_dir.glob(f"{date}COVID19MEXICO.csv"))

    if len(csvs) > 0:
        return "join"
    else:
        return "obtain_data"

def csv_to_parquet():
    import pandas as pd
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pathlib as pl
    
    import os
    import re

    data_dir = pl.Path("/home/pi/covid-data/")

    csv_list = list(data_dir.glob("*COVID19MEXICO.csv"))
    csv_list.sort(key=os.path.getctime, reverse=True)

    csv_file = csv_list[0]
    csv_date = re.findall("(\d{6})COVID19MEXICO.csv", str(csv_file))[0]

    parquet_dir = data_dir/f"{csv_date}.parquet"

    if not parquet_dir.exists():
        chunksize = 20000

        csv_stream = pd.read_csv(str(csv_file),
                                 dtype=dtypes,
                                 parse_dates=date_cols,
                                 encoding="latin-1",
                                 chunksize=chunksize)

        
        for i, chunk in enumerate(csv_stream):
            print("Chunk", i)
            metadata_collector = []
            table = pa.Table.from_pandas(df=chunk)
            
            pq.write_to_dataset(table,
                                root_path=str(parquet_dir),
                                partition_cols=["ENTIDAD_UM"],
                                metadata_collector=metadata_collector)

            pq.write_metadata(table.schema, str(parquet_dir/"_common_metadata"))

def suspect_time_series():
    import pathlib as pl
    import pyarrow.dataset as ds
    import os
    import re

    data_dir = pl.Path("/home/pi/covid-data/")

    parquets = list(data_dir.glob("*.parquet"))
    parquets.sort(key=os.path.getctime, reverse=True)

    parquet_dir = parquets[0]
    parquet_date = re.findall("(\d{6}).parquet", str(parquets))[0]

    dataset = ds.dataset(str(parquet_dir), format="parquet", partitioning="hive")
    cdmx = ds.field('ENTIDAD_UM') == 9
    sosp = ds.field("CLASIFICACION_FINAL") == 6
    df_sosp_cdmx = dataset.to_table(filter = cdmx & sosp, columns=["FECHA_INGRESO", "ORIGEN"]).to_pandas()
    ts_sosp_cdmx = df_sosp_cdmx.groupby("FECHA_INGRESO").count()["ORIGEN"]

    ts_sosp_cdmx.to_csv(f"{str(data_dir)}/{parquet_date}/sospechosos_cdmx_{parquet_date}.csv")

def confirmed_time_series():
    import pathlib as pl
    import pyarrow.dataset as ds
    import os
    import re

    data_dir = pl.Path("/home/pi/covid-data/")

    parquets = list(data_dir.glob("*.parquet"))
    parquets.sort(key=os.path.getctime, reverse=True)

    parquet_dir = parquets[0]
    parquet_date = re.findall("(\d{6}).parquet", str(parquets))[0]

    dataset = ds.dataset(str(parquet_dir), format="parquet", partitioning="hive")
    cdmx = ds.field('ENTIDAD_UM') == 9
    conf = (ds.field("CLASIFICACION_FINAL") == 1) | (ds.field("CLASIFICACION_FINAL") == 2) | (ds.field("CLASIFICACION_FINAL") == 3)
    df_conf_cdmx = dataset.to_table(filter = cdmx & conf, columns=["FECHA_INGRESO", "ORIGEN"]).to_pandas()
    ts_conf_cdmx = df_conf_cdmx.groupby("FECHA_INGRESO").count()["ORIGEN"]

    ts_conf_cdmx.to_csv(f"{str(data_dir)}/{parquet_date}/confirmados_cdmx_{parquet_date}.csv")

def negatives_time_series():
    import pathlib as pl
    import pyarrow.dataset as ds
    import os
    import re

    data_dir = pl.Path("/home/pi/covid-data/")

    parquets = list(data_dir.glob("*.parquet"))
    parquets.sort(key=os.path.getctime, reverse=True)

    parquet_dir = parquets[0]
    parquet_date = re.findall("(\d{6}).parquet", str(parquets))[0]

    dataset = ds.dataset(str(parquet_dir), format="parquet", partitioning="hive")
    cdmx = ds.field('ENTIDAD_UM') == 9
    nega = ds.field("CLASIFICACION_FINAL") == 7
    df_nega_cdmx = dataset.to_table(filter = cdmx & nega, columns=["FECHA_INGRESO", "ORIGEN"]).to_pandas()
    ts_nega_cdmx = df_nega_cdmx.groupby("FECHA_INGRESO").count()["ORIGEN"]

    ts_nega_cdmx.to_csv(f"{str(data_dir)}/{parquet_date}/negativos_cdmx_{parquet_date}.csv")

def suspect_time_series_graph():
    import pathlib as pl
    import matplotlib.pyplot as pt
    import pandas as pd
    import os
    import re

    data_dir = pl.Path("/home/pi/covid-data/")

    csv_dirs = list(data_dir.glob("??????"))
    csv_dirs.sort(key=os.path.getctime, reverse=True)

    csvs_dir = csv_dirs[0]
    csvs_date = re.findall("(\d{6})", str(csvs_dir))[0]

    df = pd.read_csv(str(data_dir/csvs_date/f"sospechosos_cdmx_{csvs_date}.csv"), parse_dates=["FECHA_INGRESO"])
    df = df.set_index("FECHA_INGRESO")

    fig = pt.figure(figsize=(10,5))
    ax = fig.gca()
    ax.plot(df, label="Raw")
    ax.plot(df.rolling(7).mean(), label="7 day rolling avg.")

    ax.set_title("Sospechosos " + csvs_date)
    ax.legend()
    ax.grid(True)

    fig.savefig(str(data_dir/csvs_date/f"sospechosos_cdmx_{csvs_date}.pdf"))

def confirmed_time_series_graph():
    import pathlib as pl
    import matplotlib.pyplot as pt
    import pandas as pd
    import os
    import re

    data_dir = pl.Path("/home/pi/covid-data/")

    csv_dirs = list(data_dir.glob("??????"))
    csv_dirs.sort(key=os.path.getctime, reverse=True)

    csvs_dir = csv_dirs[0]
    csvs_date = re.findall("(\d{6})", str(csvs_dir))[0]

    df = pd.read_csv(str(data_dir/csvs_date/f"confirmados_cdmx_{csvs_date}.csv"), parse_dates=["FECHA_INGRESO"])
    df = df.set_index("FECHA_INGRESO")

    fig = pt.figure(figsize=(10,5))
    ax = fig.gca()
    ax.plot(df, label="Raw")
    ax.plot(df.rolling(7).mean(), label="7 day rolling avg.")

    ax.set_title("Confirmados " + csvs_date)
    ax.legend()
    ax.grid(True)

    fig.savefig(str(data_dir/csvs_date/f"confirmados_cdmx_{csvs_date}.pdf"))

def negatives_time_series_graph():
    import pathlib as pl
    import matplotlib.pyplot as pt
    import pandas as pd
    import os
    import re

    data_dir = pl.Path("/home/pi/covid-data/")

    csv_dirs = list(data_dir.glob("??????"))
    csv_dirs.sort(key=os.path.getctime, reverse=True)

    csvs_dir = csv_dirs[0]
    csvs_date = re.findall("(\d{6})", str(csvs_dir))[0]

    df = pd.read_csv(str(data_dir/csvs_date/f"negativos_cdmx_{csvs_date}.csv"), parse_dates=["FECHA_INGRESO"])
    df = df.set_index("FECHA_INGRESO")

    fig = pt.figure(figsize=(10,5))
    ax = fig.gca()
    ax.plot(df, label="Raw")
    ax.plot(df.rolling(7).mean(), label="7 day rolling avg.")

    ax.set_title("Negativos " + csvs_date)
    ax.legend()
    ax.grid(True)

    fig.savefig(str(data_dir/csvs_date/f"negativos_cdmx_{csvs_date}.pdf"))

def email_data_results():
    import pathlib as pl

    import smtplib
    import ssl
    import os
    import re

    from email import encoders
    from email.mime.base import MIMEBase
    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText

    user_mail = os.environ["AIRFLOW_EMAIL"]
    admin_mail = "roberto@cad3na.com"
    password = os.environ["AIRFLOW_PASS"]

    port = 465
    smtp_server = "smtp.dreamhost.com"

    subject = "Airflow's covid-analysis DAG execution is done!"
    body = subject + " Here's your stuff!\n"

    message = MIMEMultipart()
    message["From"] = user_mail
    message["To"] = admin_mail
    message["Subject"] = subject

    message.attach(MIMEText(body, "plain"))

    data_dir = pl.Path("/home/pi/covid-data/")

    csv_dirs = list(data_dir.glob("??????"))
    csv_dirs.sort(key=os.path.getctime, reverse=True)

    csvs_dir = csv_dirs[0]

    filenames = csvs_dir.glob("*")

    for filename in filenames:
        with open(filename, "rb") as attachment:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(attachment.read())

        encoders.encode_base64(part)
        part.add_header('Content-Disposition', 'attachment', filename=str(filename).split("/")[-1])
        message.attach(part)
    
    text = message.as_string()

    context = ssl.create_default_context()

    with smtplib.SMTP_SSL(smtp_server, port, context=context) as server:
        server.login(user_mail, password)
        
        server.sendmail(user_mail, admin_mail, text)