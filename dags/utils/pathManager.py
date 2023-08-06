import os


class PathBase:

    """
    Clase abstracta para manejar el directorio de la carpeta del proyecto.

    Attributes
    ----------
        current_directory: Ruta actual de la carpeta del proyecto.

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
        current_directory = os.getcwd()

        if project_folder not in current_directory:
            current_directory = os.path.join(current_directory, project_folder)

        return current_directory

    def get_path_to(self, folder: str):
        return os.path.join(self._get_current_directory(), folder)

    def file_exist_to(self, path: str, file: str):
        file_exist = os.path.isfile("{}/{}".format(path, file))
        return file_exist


class PathDir(PathBase):

    """

    Clase principal para manejar el directorio con airflow

    """

    def _get_current_directory(self):
        airflow_directory = os.environ.get("AIRFLOW_HOME")
        project_folder = os.environ["PROJECT"]
        current_directory = os.getcwd()

        if project_folder not in current_directory:
            current_directory = os.path.join(current_directory, project_folder)

        if airflow_directory is not None:
            current_directory = os.path.join(airflow_directory, "dags")

        return current_directory
