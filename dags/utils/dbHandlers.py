import os
import pandas as pd
import numpy as np
from utils.pathManager import PathDir
from psycopg2.extras import execute_values
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

class Redshift_Handler():
    
    """
        Clase encargada de la gestion con redshift
    
    """
        
    def get_engine(self):
    
        """
            Obtiene el motor con los parametros me cargados de redshift.
        
            Returns
            -------
                engine: Objeto con el motor de redshift.
        """
    
        try:
            hook = PostgresHook(postgres_conn_id="Redshift-conn-id")
            engine = ""
            
            if hook:
                engine = hook.get_sqlalchemy_engine()
                print(engine)
            else:
                print("No se pudo establecer la conexion con Redshift")
        
        except Exception as err:
            return err
    
        return engine
    
    
    def run_script_sql(self,task_name: str, script_name: str, conection: object, data=None):
    
        """
            Ejecuta el script en Redshift, dicho script esta ubicado en la carpeta "db".

            Parameters
            ----------
                task_name (str): Obligatorio.
                    Nombre del identificador de la tarea.
                script_name (str): Obligatorio.
                    Nombre del script que se va ejecutar
                conection (object): Obligatorio.
                    Objeto con la conexion de redshift.
                data: Opcional.
                    Conjunto de datos a usar en la query. 

            Returns
            -------
                result: Resultado de la ejecucion del script
        """
        
        path = PathDir()
        db_folder = path.get_path_to("db")
        
        if conection:
            
            try:
                               
                with open("{}/{}".format(db_folder,script_name), 'r') as query:               
                                
                    if data != None:
                        
                        process = PostgresOperator(
                                    task_id=task_name,
                                    postgres_conn_id="Redshift-conn-id",
                                    sql=query.read(),
                                    parameters=data
                                )
                        result = process.execute(context={})
                    else:
                        
                        process = PostgresOperator(
                                    task_id=task_name,
                                    postgres_conn_id="Redshift-conn-id",
                                    sql=query.read(),
                                )      
                        result = process.execute(context={})                    
                 
            except Exception as err:
                result = err

        else:
            print("Conexion a Redshift fallida o no se paso la conexion...")
           
        return result
    

    def create_table(self, script_name: str, engine: object):
        
        """
           Funcion que manda a crea la tabla en redshift.

            Parameters
            ----------
            script_name (str): Obligatorio.
                Nombre del script que se va ejecutar
            engine (object): Obligatorio.
                engine: Objeto con el motor de redshift.

            Returns
            -------
                result: Resultado de la ejecucion del script
        """

        table_name = script_name.rsplit('.', 1)[0]
        message = None
        
        try:
            
            if engine:
            
                with engine.connect() as conection:
                    
                    print("Conexion con Redshift: OK")
                    print("Verificando la existencia de tabla {}...".format(table_name))
                    message = ""
                    istable = self.run_script_sql("is-Exist-table","isTableExist.sql", conection)

                    if istable:
                        print("La tabla {} ya fue creada o existe...".format(table_name))
                        message = False
                    else:
                        
                        print("La tabla {} no existe en el esquema, se procede a crearla.".format(table_name) )
                        result = self.run_script_sql("create-table",script_name, conection)
                        message = result
            
            else:
                print("El archivo .env no se encuentra...")
                
        except Exception as err:
            message = err
            
        return message            
                
                
    def write_df(self, file_name: str, engine: object):
        
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
        
        table_name = "articles"
        schema = os.environ["REDSHIFT_USER"]
        count_rows = 0
        exist_rows = 0
        df = pd.read_csv(file_name)
        df_columns_list = df.columns.tolist()
        
        insert_query ="insert into {}.{} ( {} ) values  %s  ".format(schema, 
                                                                     table_name, 
                                                                     ", ".join(df_columns_list))
        
        print("Cantidad de Articulos a guardar: {} ".format(len(df)))
        
        if engine:
            
            with engine.connect() as conection:
                
                cursor = conection.connection.cursor()

                if conection.dialect.has_table(conection, table_name): 
                    
                    print("Consultando la existencia de articulos en Redshift...")
                    
                    df["publishedAt"] =pd.to_datetime(df["publishedAt"])
                    df.replace(np.nan, None)
                    
                    for i ,row in df.iterrows():
                        
                        result = self.run_script_sql("is-Exist-Record","beforeQuery.sql", conection, data=row.to_dict())
                        if result:
                            count_rows+=1  
                            execute_values(cursor, 
                                          insert_query, 
                                           [tuple(row)])
                        else:
                            exist_rows+=1
                        
                    print("Cantidad de Articulos existentes: {} ".format(exist_rows))
                    print("Cantidad de Articulos insertados: {}".format(count_rows))        
                    conection.connection.commit()
                    cursor.close()  
                
                else:
                    print("La tabla {} aun no se ha creado".format(table_name)) 
        
        else:
            print("El archivo .env no se encuentra...")            
  


    