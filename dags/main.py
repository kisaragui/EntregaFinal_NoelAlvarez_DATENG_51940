import os
import time
import json
from utils.dbHandlers import Redshift_Handler
from utils.dataHandlers import Articles
from utils.pathManager import PathDir
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator

# Parametros para la api 

url = os.environ["TOP_HEADLINES_URL"]
api_params = {}
api_params["country"] = "ar"
api_params["category"] = "technology"
api_params["pageSize"] = 100
api_params["apikey"] = os.environ["KEY"]

# Inicializando clases

redshift = Redshift_Handler()
articulos = Articles()
path = PathDir()
file_name = "Articles"
data_path = path.get_path_to("data")
date = time.strftime('%Y-%m-%d-%H:%M:%S', time.localtime())
file_path = "{}/{}_{}".format(data_path,file_name, date)

# Obteniendo la conexion de redshift

conexion = redshift.get_engine()


@dag(os.environ["PROJECT"], 
        default_args = {
            'owner': 'airflow',
            'depends_on_past': False,
            'retry_delay': timedelta(minutes=5),
            "start_date": datetime(2023,7,20),
        },
        catchup = False,
        schedule_interval= '0 0 * * *'
)
def main():
    
    """
    
    Función principal que ejecuta los tasks 

    """
   
    #task de inicio
    start = EmptyOperator(task_id ="Start")
    
    # Ejecutando la api con los parametros indicados
    @task()
    def call_api_task(file_path ,url, prs):
        file_json = "{}.json".format(file_path)
        extract = articulos.call_api(url,args=prs)
        with open(file_json, "w") as file:
            file.write(json.dumps(extract))
        
        print("Articulos localizados: {}".format(file_json))
        return file_json
    
    # Limpiando  y transformando la informacion obtenida
    @task()
    def normalize_data_task(file_path):
        
        file_csv = file_path.replace("json", "csv")
        transforms = articulos.normalize_data(file_path)
        transforms.to_csv(file_csv, index=False)
        
        print("Informacion normalizada: {}".format(file_csv))
        return file_csv

    # Creando la tabla en redshift
    @task()
    def create_table_task(script_name, conexion):
        return redshift.create_table(script_name, conexion)

    # Insertando la información obtenida en la tabla previamente create
    @task()
    def load_task(file_path, conexion):
        load =  redshift.write_df(file_path,conexion)
        return file_path
    
    @task()
    def remove_trash_task(data_path, file_path):
        
        print("Identificando los archivos procesados...")
        for file in os.listdir(data_path):
            if file.split(".")[0] == file_path.split("/")[-1].split(".")[0]:
                print("Borrando archivo {}".format(file))
                os.remove("{}/{}".format(data_path,file))

        
    # Ultimo task
    finish = EmptyOperator(task_id ="Finish")
    
    call_api = call_api_task(file_path, url, api_params)
    clean_data = normalize_data_task(call_api)
    create_table = create_table_task("articles.sql", conexion)
    insert_data = load_task(clean_data, conexion)
    remove_data = remove_trash_task(data_path, insert_data)
    
    #Ordenar de ejecucion de los dags 
    start >> create_table >>  call_api >> clean_data >> insert_data >> remove_data >> finish

main()
    