import requests as r
import pandas as pd
import itertools
import json


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
            print("No se subministro la Key de la Api")
            return None

        FILE_JSON = "{}.json".format(file_path)
        all_news = []
        count_articles = 0

        # Se llama a la api por cada valor de la categoria y el pais
        for country_item, category_item in itertools.product(
            args["country"], args["category"]
        ):
            params = {
                "country": country_item,
                "category": category_item,
                "pageSize": args["pageSize"],
                "apikey": args["apikey"],
            }

            try:
                response = r.get(url, params=params)
                result = response.json()

                if response.status_code == 200 and result["status"] == "ok":
                    template_json = {
                        "country": country_item,
                        "category": category_item,
                        "articles": result["articles"],
                    }
                    
                    all_news.append(template_json)
                    count_articles += len(result["articles"])

                elif result["status"] == "ok" and result["totalResults"] == 0:
                    raise ValueError("No se encontraron Articulos")

            except:
                raise Exception(result["message"])

        if all_news:
            print("Cantidad de Articulos obtenidos: {}".format(count_articles))
            result = {"result": all_news}

            with open(FILE_JSON, "w") as file:
                file.write(json.dumps(result))

            print("Articulos localizados: {}".format(FILE_JSON))

        return FILE_JSON

    def normalize_data(self, data_path: str):
        """
            Funcion que aplica el tratamiento corresponde en la informacion y
            la devuelde en formato de dataframe

        Parameters
        ----------
            data_path (str), Obligatorio.
                Directorio del archivo en formato json con los articulos encontrados por la api.

        Returns
        -------
            FILE_CSV:  
                Ubicacion del archivo en formato csv con informacion normalizada.
        """

        if not data_path:
            print("No se paso la ubicacion del archivo de los articulos...")
            return None

        try:
            FILE_CSV = data_path.replace("json", "csv")
            print("Aplicando transformaciones y ajustes...")

            with open(data_path, "r") as file:
                raw_data = json.loads(file.read())

            df_temp = pd.json_normalize(
                raw_data["result"], record_path="articles", meta=["country", "category"]
            )

            df_temp["publishedAt"] = df_temp["publishedAt"].apply(
                lambda x: x.rsplit("T", 1)[0]
            )
            df_temp["author"].fillna("anonymous", inplace=True)
            df_temp.insert(0, "id", df_temp.reset_index().index + 1)
            df_temp.insert(1, "SourceId", df_temp["source.name"].apply(
                lambda x: x.lower().replace(" ", "-"))
            )
            df_final = df_temp.rename(columns={"source.name": "sourceName"})
            df_final.drop(columns="source.id")
            df_final.to_csv(FILE_CSV, index=False)

            print("Informacion normalizada: {}".format(FILE_CSV))

        except Exception as err:
            raise Exception(err)

        return FILE_CSV
