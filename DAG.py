import os.path
import sys

import pendulum
from airflow import DAG
from airflow.decorators import task

sys.path.insert(0, os.path.dirname(__file__))

with DAG(
    "anime_rank",
    description="DAG extracao dos 100 priemiros animes",
    start_date=pendulum.datetime(2024, 1, 3, tz="America/Sao_Paulo"),
    schedule_interval="0 20 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["anime", "extracao"],
) as dag:
    @task
    def extract():
        import pandas as pd
        from extract_data import ExtractData



        lista_dicionarios = ExtractData.extracao()

        df = pd.DataFrame(lista_dicionarios)

        return df


    @task
    def transform(df):
        df.fillna("Nada aqui", inplace=True)
        df = df.applymap(lambda x: str(x).replace('°', ""))
        df = df.applymap(lambda x: str(x).replace("'", ""))
        return df


    @task
    def load(df):
        from load_data import LoadData
        LoadData.load_to_postgres(df)

    ext = extract()
    tf = transform(ext)
    ld = load(tf)

    ext >> tf >> ld
