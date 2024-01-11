class LoadData:
    @staticmethod
    def load_to_postgres(df):
        import psycopg2.extras
        from airflow.hooks.base import BaseHook

        servidor = BaseHook.get_connection(conn_id="postgres_server")

        conn = psycopg2.connect(
            host=servidor.host,
            port=servidor.port,
            dbname=servidor.schema,
            user=servidor.username,
            password=servidor.password,
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
