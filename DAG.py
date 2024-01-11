import pendulum
from airflow import DAG
from airflow.decorators import task

with DAG(
    "anime rank",
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

        lista_dicionarios = []
        urls = ['https://myanimelist.net/topanime.php?limit=0', 'https://myanimelist.net/topanime.php?limit=50']

        for url in urls:
            print(f'Extrayendo datos de: {url}')
            mapped = ExtractData.extracao(url)
            lista_dicionarios.append(mapped)

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
