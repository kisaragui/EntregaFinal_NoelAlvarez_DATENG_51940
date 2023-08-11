import os
import smtplib
from email.mime.text import MIMEText
import spacy
import pandas as pd
from collections import Counter
from utils.dbHandlers import RedshiftHandlers
from airflow.models import Variable


class SMTPManager:
    def __init__(self):
        self.smtp_email_from = Variable.get("SMTP_EMAIL_FROM")
        self.smtp_email_to = Variable.get("SMTP_EMAIL_TO")
        self.smtp_host = Variable.get("SMTP_HOST")
        self.smtp_password = Variable.get("SMTP_PASSWORD")
        self.smtp_port = Variable.get("SMTP_PORT")

    def on_success_function(self, context):
        """
        Funcion para enviar un email cuando sea exitosa la ejecucion del dag

        Parameters
        ----------
            context: (dict)
                Diccionario con referencias a los objetos de la task
        """
        content = ""
        print("Ejecutando on_success_callback!!!!")
        backfill = os.environ.get("BACKFILL")
        reprocess_date = os.environ.get("REPROCESS_DATE")

        content += "Detalles de la ejecucion del dag: \n"
        content += "Nombre del Dag: {}\n".format(context["dag"].dag_id)
        content += "Fecha de ejecucion: {}\n\n".format(context["ts"])

        if backfill == "True":
            content += "Mecanismo de backfill: Activado\n"
            content += "Fecha a reprocesar: {}\n".format(reprocess_date)
        else:
            content += "Mecanismo de backfill: Desactivado\n"
        content += "Valores de task Retornados: \n"

        for task_id in context["dag"].task_ids:
            content += "{}: {}\n".format(
                task_id, context["ti"].xcom_pull(key="return_value", task_ids=task_id)
            )
        subject = "Dag {} ejecucion exitosa!".format(context["dag"].dag_id)
        self.send_email(content, subject)

    def on_failure_function(self, context):
        """
        Funcion para enviar un email cuando sea fallida la ejecucion del dag

        Parameters
        ----------
            context: (dict)
                Diccionario con referencias a los objetos de la task
        """
        print("Lanzando con callback on_failure_function!!!!")

        content = ""
        backfill = os.environ.get("BACKFILL")
        reprocess_date = os.environ.get("REPROCESS_DATE")

        content += "Hubo un Error en la ejecucion del dag!\n"
        content += "Detalles de la ejecucion del dag: \n"
        content += "Nombre del Dag: {}\n".format(context["dag"].dag_id)
        content += "Fecha de ejecucion: {}\n\n".format(context["ts"])

        if backfill == "True":
            content += "Mecanismo de backfill: Activado\n"
            content += "Fecha a reprocesar: {}\n".format(reprocess_date)
        else:
            content += "Mecanismo de backfill: Desactivado\n"
        content += "Task del error: \n"
        content += "Task id: {}\n".format(context["ti"].task_id)
        content += "Descripcion del error: {}\n".format(context.get("exception"))
        content += "Link del log: {}".format(context["ti"].log_url)
        subject = "Dag {} ejecucion fallida!".format(context["dag"].dag_id)
        self.send_email(content, subject)

    def send_email(self, content: str, subject: str):
        """
        Funcion que envia un email con el estado de la ejecucion del dag.

        Parameters
        ----------
        content: (str)
            Contenido del email
        subject: (str)
            Asunto o en cabezado del email
        """
        try:
            x = smtplib.SMTP(self.smtp_host, self.smtp_port)
            x.starttls()
            x.login(self.smtp_email_from, self.smtp_password)

            body = "Subject: {}\n\n{}".format(subject, content)
            x.sendmail(self.smtp_email_from, self.smtp_email_to, body)
            print("Email enviado")

        except Exception as err:
            print("Fallo al enviar el Email")
            raise Exception(err)

    def send_notification_threshold(
        self, word_freq: dict, max_threshold: int, min_threshold: int
    ):
        """
            Funcion que envia la notificacion del umbral.

         Parameters
        ----------
            word_freq: (dict)
                Diccionario con la informacion por idioma de los titulos de los articulos.
            max_threshold: (int)
                Valor maximo del umbral
            min_threshold: (int)
                Valor minino del umbral
        """
        content = "Estas son las palabras mas frecuentes entre los titulos de los articulos:\n"
        en_words_above_threshold = [
            "Palabra: {}, Frecuencia: {}\n".format(word, freq)
            for word, freq in word_freq["en"]
            if min_threshold <= freq <= max_threshold
        ]
        if en_words_above_threshold:
            content += "En el idioma Ingles: \n"
            content += "".join(en_words_above_threshold)
        else:
            content += "En el idioma Ingles: \n"
            content += "Las frecuencias no superan esta categoria\n"

        es_words_above_threshold = [
            "Palabra: {}, Frecuencia: {}\n".format(word, freq)
            for word, freq in word_freq["es"]
            if min_threshold <= freq <= max_threshold
        ]
        if es_words_above_threshold:
            content += "En el idioma Espaniol: \n"
            content += "".join(es_words_above_threshold)
        else:
            content += "En el idioma Espaniol: \n"
            content += "Las frecuencias no superan esta categoria\n"

        if en_words_above_threshold or es_words_above_threshold:
            subject = "Notificacion de Umbral"
            self.send_email(content, subject)

    def check_threshold(
        self, date_published: str, max_threshold: int, min_threshold: int
    ):
        """
            Funcion encarga de obtener la informacion para chequear el umbral.

        Parameters
        ----------
            date_published: (str)
                Fecha del dia de la puplicacion, corrientemente seria la fecha
                subministrada por la api
            max_threshold: (int)
                Valor maximo del umbral
            min_threshold: (int)
                Valor minino del umbral
        """
        REDSHIFT = RedshiftHandlers()
        df_columns = ["publishedAt", "title", "language"]

        query = REDSHIFT.run_script_sql(
            "get_data", "checkThreshold.sql", data={"date_published": date_published}
        )
        df = pd.DataFrame(query, columns=df_columns)

        nlp_en = spacy.load("en_core_web_sm")
        nlp_es = spacy.load("es_core_news_sm")
        en_words = []
        es_words = []

        for i, row in df.iterrows():
            if row["language"] == "en":
                doc = nlp_en(row["title"].lower())
                words = [
                    token.text for token in doc if token.is_alpha and not token.is_stop
                ]
                en_words.extend(words)
            else:
                doc = nlp_es(row["title"].lower())
                words = [
                    token.text for token in doc if token.is_alpha and not token.is_stop
                ]
                es_words.extend(words)

        en_word_freq = Counter(en_words).most_common()
        es_word_freq = Counter(es_words).most_common()

        word_freq = {"en": en_word_freq, "es": es_word_freq}
        self.send_notification_threshold(word_freq, max_threshold, min_threshold)
