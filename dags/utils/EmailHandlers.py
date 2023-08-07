from airflow.models import Variable
import os
import smtplib


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
