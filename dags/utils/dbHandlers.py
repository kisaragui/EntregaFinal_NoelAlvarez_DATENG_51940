import os
import pandas as pd
import numpy as np
from utils.pathManager import PathDir
from psycopg2.extras import execute_values
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class RedshiftHandlers:

    """
    Clase encargada de la gestion con redshift

    """

    def run_script_sql(self, task_name: str, script_name: str, data=None):
        """
        Ejecuta el script en Redshift, dicho script esta ubicado en la carpeta "db".

        Parameters
        ----------
            task_name (str): Obligatorio.
                Nombre del identificador de la tarea.
            script_name (str): Obligatorio.
                Nombre del script que se va ejecutar.
            data: Opcional.
                Conjunto de datos a usar en la query.

        Returns
        -------
            result: Resultado de la ejecucion del script
        """

        path = PathDir()
        db_folder = path.get_path_to("db")

        try:
            with open("{}/{}".format(db_folder, script_name), "r") as query:
                sql = query.read()

                if data is not None:
                    process = PostgresOperator(
                        task_id=task_name,
                        postgres_conn_id="Redshift-conn-id",
                        sql=sql,
                        parameters=data,
                    )
                    result = process.execute(context={})
                else:
                    process = PostgresOperator(
                        task_id=task_name,
                        postgres_conn_id="Redshift-conn-id",
                        sql=sql,
                    )
                    result = process.execute(context={})

        except Exception as err:
            raise Exception(err)

        return result

    def write_df(self, file_name: str):
        """
        Funcion que se encarga de guardar el contenido del archivo en formato csv a la tabla
        en redshift, consulta primero si ya estan insertado y guarda los que no existe.

        Parameters
        ----------
            file_name (str): Obligatorio.
                Directorio del archivo en formato csv con los articulos encontrados a insertar en la tabla.
            engine (object): Obligatorio.
                Objeto con el motor de redshift.

        """
        count_rows = 0
        exist_rows = 0
        hook = PostgresHook(postgres_conn_id="Redshift-conn-id")
        engine = hook.get_sqlalchemy_engine()
        CONNECTION = engine.connect()
        TABLE_NAME = "articles"
        SCHEMA = os.environ["REDSHIFT_USER"]
        DF = pd.read_csv(file_name)
        DF_COLUMNS_LIST = DF.columns.tolist()


        INSERT_QUERY = "insert into {}.{} ( {} ) values  %s  ".format(
            SCHEMA, TABLE_NAME, ", ".join(DF_COLUMNS_LIST)
        )

        print("Cantidad de Articulos a guardar: {} ".format(len(DF)))

        if CONNECTION.dialect.has_table(CONNECTION, TABLE_NAME):
            with CONNECTION.begin() as trans:
                with CONNECTION.connection.cursor() as cursor:
                    print("Consultando la existencia de articulos en Redshift...")
                    DF["publishedAt"] = pd.to_datetime(DF["publishedAt"])
                    DF.replace(np.nan, None)

                    for i, row in DF.iterrows():
                        result = self.run_script_sql(
                            "is-Exist-Record", "isExistRow.sql", data=row.to_dict()
                        )
                        if result:
                            count_rows += 1
                            execute_values(cursor, INSERT_QUERY, [tuple(row)])
                        else:
                            exist_rows += 1

            print("Cantidad de Articulos existentes: {} ".format(exist_rows))
            print("Cantidad de Articulos insertados: {} ".format(count_rows))

        else:
            print("La tabla {} aun no se ha creado".format(TABLE_NAME))

        return file_name
