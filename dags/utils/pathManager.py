import os


class PathBase():
            
    """
        Clase abstracta de manejar el directorio de la carpeta de entregables.
        
        Attributes
        ----------
            directory: Ruta concadenada a la carpeta del primer entregable
            
        Methods
        -------
            _get_current_directory: 
                Obtiene la ruta raiz del proyecto.

            get_path_to(carpeta: string):
                Obtiene la ruta de carpeda pasada.
            
            file_exist_to(ruta: string, archivo: string):
                Devuelve un booleando si existe el archivo en el ruta indicada.
            
    """
        
    def __init__(self):
        
        self.current_directory = self._get_current_directory()
        

    def _get_current_directory(self):
        
        project_folder = os.environ["PROJECT"]
        
        if project_folder in os.getcwd():
            
            path_absolute = os.getcwd()
            
        else:    
            
            path_absolute = os.path.join(os.getcwd(), project_folder)
            
        return  path_absolute
    
          
    def get_path_to(self, folder: str):
        
        return os.path.join(self._get_current_directory(), folder)
    
     
    def file_exist_to(self,path: str, file: str):
        
        file_exist = os.path.isfile('{}/{}'.format(path, file))

        return file_exist
    

class PathDir(PathBase):
    
    
    
    def _get_current_directory(self):
    
        airflow_directory = os.environ["AIRFLOW_HOME"]
        
        project_folder = os.environ["PROJECT"]
        
        if project_folder in os.getcwd():
            
            path_absolute = os.getcwd()
            
        else:    
            
            path_absolute = os.path.join(os.getcwd(), project_folder)
            
        
        if airflow_directory != None:
            
            path_absolute = os.path.join(airflow_directory, "dags")
            
        
        return  path_absolute