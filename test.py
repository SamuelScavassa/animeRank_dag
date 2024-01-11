import requests
from bs4 import BeautifulSoup
import pandas as pd
import psycopg2.extras

map = [
"clasificacion",
"titulo_anime",
"info_anime",
"puntuacion_anime",
"href",
"Synonyms:",
"Japanese:",
"English:",
"Type:",
"Episodes:",
"Status:",
"Aired:",
"Premiered:",
"Broadcast:",
"Producers:",
"Licensors:",
"Studios:",
"Source:",
"Genres:",
"Demographic:",
"Duration:",
"Rating:",
"Score:",
"Ranked:",
"Popularity:",
"Members:",
"Favorites:"
]


def extraer_datos(url):
    response = requests.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')
        filas = soup.find_all('tr', class_='ranking-list')

        for fila in filas:
            result_dict = {}
            clasificacion = fila.find('span', class_='lightLink').text.strip()
            result_dict["clasificacion"] = clasificacion
            titulo_anime = fila.find('h3', class_='anime_ranking_h3').text.strip()
            result_dict["titulo_anime"] = titulo_anime
            info_anime = fila.find('div', class_='information').text.strip()
            result_dict["info_anime"] = info_anime
            puntuacion_anime = fila.find('span', class_='score-label').text.strip()
            result_dict["puntuacion_anime"] = puntuacion_anime
            href = fila.find('a', class_='hoverinfo_trigger')['href']
            result_dict["href"] = href
            response = requests.get(href)

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')

                # Restante do código...

                spaceit_pad_elements = soup.find_all('div', class_='spaceit_pad')

                # Iterar sobre os elementos encontrados
                for element in spaceit_pad_elements:
                    text_parts = element.stripped_strings
                    key = next(text_parts)
                    value = ' '.join(text_parts)
                    value = value.replace("'", '')
                    value = value.replace(",", ".")
                    result_dict[key] = value

                # Adicione os resultados à lista de dicionários
                mapped = {}
                for value in result_dict:
                  if value in map:
                    mapped[value] = result_dict[value]
                lista_dicionarios.append(mapped)



    else:
        print(f'Error al realizar la solicitud para {url}. Código de estado: {response.status_code}')

# Lista de URLs de las páginas
urls = ['https://myanimelist.net/topanime.php?limit=0', 'https://myanimelist.net/topanime.php?limit=50']

# Lista para armazenar os dicionários
lista_dicionarios = []

# Iterar sobre las URLs y extraer datos
for url in urls:
    print(f'Extrayendo datos de: {url}')
    extraer_datos(url)

# Criar DataFrame a partir da lista de dicionários
df = pd.DataFrame(lista_dicionarios)

df.fillna("Nada aqui", inplace=True)
df = df.applymap(lambda x: str(x).replace('°', ""))
df = df.applymap(lambda x: str(x).replace("'", ""))

user = 'postgres'
password = 'samuel04'
host = '192.168.1.80'
port = '5432'
database = 'postgres'



# Crie uma conexão com o banco de dados
conn = psycopg2.connect(
    host=host,
    port=port,
    dbname=database,
    user=user,
    password=password,
)

tablename = "airflow.anime_rank"

columns_declaration = ", ".join([f'"{col_name}" VARCHAR(10000)' for col_name in df.columns])

with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
    cursor.execute(
        f"""
            CREATE TABLE IF NOT EXISTS {tablename} (
                {columns_declaration}
            );
        """
    )
    cursor.execute(f"""TRUNCATE TABLE {tablename};""")

with conn, conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
    lista = df.values.tolist()
    for x in lista:
        cursor.execute(
            f"""
            INSERT INTO {tablename} 
            VALUES {tuple(x)};
            """
        )
        print('ok')
