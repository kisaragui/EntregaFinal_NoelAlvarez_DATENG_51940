import requests as r
import pandas as pd
import numpy as np
import json
import csv
import os
from utils.pathManager import PathDir


class Articles:

    """
    Clase encargada de gestion de los Articulos.

    """

    def call_api(self, file_path: str, url: str, args=None):
        """
        Realiza la solucitud de la api y devuelve los articulos mas populares segun
        la categoria  y la fecha para luego guardarlo en un archivo en formato json.

        Parameters
        ----------
        file_path: str, Obligatorio.
            Ruta absoluta donde se guardara el archivo en formato json.
        url: str, Obligatorio.
            Endpoint de la api
        args: Obligatorio.
            Son los parametros a utilizar para filtrar la solicitud.

        Returns
        -------
            FILE_JSON:
               Ubicacion del archivo en formato json con los articulos encontrados.

            NOTA: Es necesario pasar por el parametro "args" la clave de autenticacion
                de la api (apikey).

        """

        if "apikey" not in args:
            raise ValueError("No se subministro la Key de la Api")

        BACKFILL = os.environ["BACKFILL"]
        FILE_JSON = "{}.json".format(file_path)
        all_news = []
        filtered_articles = []
        count_articles = 0

        # Se llama a la api por cada valor de la categoria y el pais
        try:
            for language_item in args["language"]:
                params = args.copy()
                params["language"] = language_item

                if BACKFILL == "True":
                    params["from_param"] = os.environ["REPROCESS_DATE"]

                print("paramentros de la api: ", params)
                response = r.get(url, params=params)
                result = response.json()

                if result["status"] == "ok" and result["totalResults"] > 0:
                    filtered_articles = [
                        article
                        for article in result["articles"]
                        if params["from_param"] in article["publishedAt"]
                    ]
                    if filtered_articles:
                        template_json = {
                            "language": language_item,
                            "articles": filtered_articles,
                        }
                        all_news.append(template_json)
                        count_articles += len(filtered_articles)

                elif result["status"] == "ok" and result["totalResults"] == 0:
                    raise ValueError("No se encontraron Articulos")

        except Exception as err:
            raise Exception(err)

        if all_news:
            print("Cantidad de Articulos obtenidos: {}".format(count_articles))
            result = {"result": all_news}

            with open(FILE_JSON, "w") as file:
                file.write(json.dumps(result))

            print("Articulos localizados: {}".format(FILE_JSON))

        return FILE_JSON

    def normalize_data(self, data_path: str):
        if not data_path:
            raise ValueError("No se paso la ubicacion del archivo de los articulos...")

        path = PathDir()
        data_directory = path.get_path_to("data")
        source_json = "{}/sources.json".format(data_directory)
        try:
            FILE_CSV = data_path.replace("json", "csv")

            print("Aplicando transformaciones y ajustes...")

            with open(data_path, "r") as file_json, open(
                source_json, "r"
            ) as file_source:
                raw_data = json.load(file_json)
                raw_source = json.load(file_source)

            df_temp = pd.json_normalize(
                raw_data["result"], record_path="articles", meta="language"
            )
            df_sources = pd.DataFrame.from_dict(raw_source["sources"])

            df_merged = df_temp.merge(
                df_sources[["name", "category", "country"]],
                left_on="source.name",
                right_on="name",
                how="left",
            )

            df_merged["publishedAt"] = df_merged["publishedAt"].apply(
                lambda x: x.rsplit("T", 1)[0]
            )
            df_merged["author"].fillna("anonymous", inplace=True)
            df_merged["category"].fillna("general", inplace=True)
            df_merged.insert(0, "id", df_temp.reset_index().index + 1)
            df_merged.insert(
                1,
                "SourceId",
                df_temp["source.name"].apply(lambda x: x.lower().replace(" ", "-")),
            )
            df_renamed = df_merged.rename(columns={"source.name": "sourceName"})
            df_final = df_renamed.drop(columns=["source.id", "name"])
            df_final.replace(np.nan, None)
            df_final.to_csv(
                FILE_CSV, index=False, quoting=csv.QUOTE_ALL, doublequote=True
            )

            print("Informacion normalizada: {}".format(FILE_CSV))

        except Exception as err:
            raise Exception(err)

        return FILE_CSV
