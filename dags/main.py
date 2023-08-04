import os
from utils.dbHandlers import RedshiftHandlers
from utils.dataHandlers import Articles
from utils.pathManager import PathDir
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

REDSHIFT = RedshiftHandlers()
ARTICULOS = Articles()
path = PathDir()
data_path = path.get_path_to("data")
file_name = "Articles"
date = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")

FILE_PATH = "{}/{}_{}".format(data_path, file_name, date)
URL = Variable.get("TOP_HEADLINES_URL")
API_PARAMS = {
    "country": ["ar", "us"],
    "category": ["technology"],
    "pageSize": 100,
    "apikey": Variable.get("APIKEY")
}


@dag(
    "Entregafinal",  # os.environ["PROJECT"],
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retry_delay": timedelta(minutes=5),
        "start_date": datetime(2023, 7, 20),
    },
    catchup=False,
    schedule_interval="0 0 * * *",
)
def main():
    """

    Función principal que ejecuta los tasks

    """

    # task de inicio
    start = EmptyOperator(task_id="Start")

    # Ejecutando la api con los parametros indicados
    @task()
    def call_api_task(file_path, url, prs):
        return ARTICULOS.call_api(file_path, url, args=prs)

    # Limpiando  y transformando la informacion obtenida
    @task()
    def normalize_data_task(file_path):
        return ARTICULOS.normalize_data(file_path)

    # Creando la tabla en redshift
    @task()
    def create_table_task(script_name):
        return REDSHIFT.run_script_sql("create-table", script_name)

    # Insertando la información obtenida en la tabla previamente create
    @task()
    def insert_data_task(file_path):
        return REDSHIFT.write_df(file_path)

    @task()
    def remove_data_task(data_path, file_path):
        print("Identificando los archivos procesados...")
        for file in os.listdir(data_path):
            if file.split(".")[0] == file_path.split("/")[-1].split(".")[0]:
                print("Borrando archivo {}".format(file))
                os.remove("{}/{}".format(data_path, file))

    # Ultimo task
    finish = EmptyOperator(task_id="Finish")

    create_table = create_table_task("articles.sql")
    call_api = call_api_task(FILE_PATH, URL, API_PARAMS)
    normalize_data = normalize_data_task(call_api)
    insert_data = insert_data_task(normalize_data)
    remove_data = remove_data_task(data_path, insert_data)

    # Ordenar de ejecucion de los dags
    start >> create_table >> call_api >> normalize_data
    
    insert_data >> remove_data >> finish



main()
