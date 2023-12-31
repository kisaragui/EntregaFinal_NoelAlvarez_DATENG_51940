import os
from utils.dbHandlers import RedshiftHandlers
from utils.dataHandlers import Articles
from utils.pathManager import PathDir
from utils.EmailHandlers import SMTPManager
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

SMTP = SMTPManager()
REDSHIFT = RedshiftHandlers()
ARTICULOS = Articles()
path = PathDir()
data_directory = path.get_path_to("data")
file_name = "Articles"
CURRENT_DATE = datetime.now().strftime("%Y-%m-%d-%H:%M:%S")

# Configuraciones para la api
TEXT_SEARCH = "chatgpt"
LANGUAGES = ["es", "en"]

# Configuraciones para Del threshold
THRESHOLD = True
THRESHOLD_DATE = "2023-08-04"
MAX_THRESHOLD = 50
MIN_THRESHOLD = 10

# Variables del backfill
BACKFILL = os.environ.get("BACKFILL")
REPROCESS_DATE = os.environ.get("REPROCESS_DATE")

# Variables de la api
FILE_PATH = "{}/{}_{}".format(data_directory, file_name, CURRENT_DATE)
URL = Variable.get("TOP_HEADLINES_URL")
API_PARAMS = {
    "q": TEXT_SEARCH,
    "language": LANGUAGES,
    "pageSize": 100,
    "from_param": CURRENT_DATE,
    "apikey": Variable.get("APIKEY"),
    "sortby": "publishedAt",
}


@dag(
    os.environ["PROJECT"],
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "retry_delay": timedelta(minutes=5),
        "start_date": CURRENT_DATE,
        "on_failure_callback": SMTP.on_failure_function,
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

    # Creando la tabla en redshift
    @task()
    def create_table_task(script_name):
        return REDSHIFT.run_script_sql("create-table", script_name)

    # Mecanismo backfilling
    @task.branch(task_id="is_backfill")
    def branch_task(backfill):
        print("parameter BACKFILL: ", backfill)
        if backfill == "True":
            return "backfill_task"
        else:
            return "call_api_task"

    @task(trigger_rule="one_success")
    def backfill_task(script_name, reprocess_date):
        return REDSHIFT.run_script_sql(
            "reprocess_task", script_name, data={"reprocess_date": reprocess_date}
        )

    # Ejecutando la api con los parametros indicados
    @task(trigger_rule="one_success")
    def call_api_task(file_path, url, prs):
        return ARTICULOS.call_api(file_path, url, args=prs)

    # Limpiando  y transformando la informacion obtenida
    @task()
    def normalize_data_task(file_path):
        return ARTICULOS.normalize_data(file_path)

    # Guardando la informacion en redshift
    @task()
    def insert_data_task(file_path):
        return REDSHIFT.write_df(file_path)

    # Chequea el umbral y envia un email
    @task()
    def check_threshold_task():
        if THRESHOLD:
            return SMTP.check_threshold(THRESHOLD_DATE, MAX_THRESHOLD, MIN_THRESHOLD)
        else:
            return None

    # Eliminando los archivos procesados
    @task()
    def remove_data_task(data_path, file_path):
        print("Identificando los archivos procesados...")
        for file in os.listdir(data_path):
            if file.split(".")[0] == file_path.split("/")[-1].split(".")[0]:
                print("Borrando archivo {}".format(file))
                os.remove("{}/{}".format(data_path, file))

    # Ultimo task
    finish = EmptyOperator(
        task_id="Finish",
        trigger_rule="none_failed",
        on_success_callback=SMTP.on_success_function,
    )

    create_table = create_table_task("articles.sql")
    branch = branch_task(BACKFILL)
    backfill = backfill_task("removeRow.sql", REPROCESS_DATE)
    call_api = call_api_task(FILE_PATH, URL, API_PARAMS)
    normalize_data = normalize_data_task(call_api)
    insert_data = insert_data_task(normalize_data)
    check_threshold = check_threshold_task()
    remove_data = remove_data_task(data_directory, insert_data)

    # Ordenar de ejecucion de los dags
    start >> create_table >> branch >> [backfill, call_api]
    (
        backfill
        >> call_api
        >> normalize_data
        >> insert_data
        >> check_threshold
        >> remove_data
        >> finish
    )


main()
