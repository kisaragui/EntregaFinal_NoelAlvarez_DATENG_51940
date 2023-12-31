# Proyecto Final del Curso de Data Engineer Flex - Comision 51940

Se uso la api newsapi.org como requerimiento para desarrollo del entregable. 
A continuacion esta el enlace de la pagina:

https://newsapi.org/

Se decidio Cambiar el endpoint "everything".

Dicha api obtiene articulos de diferentes sitios web y blogs segun los parametros subministrados.

## Configuraciones

Las siguientes variables fueron diseñadas para configurar y activar diferentes funcionalidades de codigo del proyecto.

En caso de la API:
* TEXT_SEARCH: `Palabra o frase que relacionada al articulo.`
* LANGUAGES: `Lista o cadena de texto de los idiomas de los articulos a filtrar, ejemplo: es, en, pr, etc.`

En caso del threshold
* THRESHOLD: `Activa o desactiva la notificacion del umbral. el formato es un booleano: True o False.`
* THRESHOLD_DATE: `Fecha de los articulos publicados a obtener, puede ser en formato YYY-MM-DD.`
* MAX_THRESHOLD : `Valor  maximo del umbral de tipo numerico.`
* MIN_THRESHOLD : `Valor  minimo del umbral de tipo numerico.`

 ## Mecanismo Backfill:

  Para efectuar este mecanismo, se proporciona las variables de entorno BACKFILL y REPROCESS_DATE, ubicadas en el archivo .env (mas adelante se indica como recrearlo).

  A continuacion se indican algunos criterios para usarlas:
  
  * BACKFILL : Setearlo con los valores "True" o "False" (incluir las comillas), por defecto deber estar en "False".
  * REPROCESS_DATE Debe ser una fecha anterior a la actual, maximo 3 semanas de diferencia y en formato de FECHA SIN HORAS.

  La api en la version gratuita, solo proporciona la historia de los articulos de 3 semanas pasadas.


## Pasos a seguir: 

Una vez clonado el repositorio, se debe tener en cuenta lo siguiente:

* Crear un archivo ".env":
  
  Dicho archivo contiene algunas variables de entorno necesarias para el funcionamento del proyecto. 
  El Archivo ".env_template", es una plantilla de como deberia estar formado el archivo .env.
  Es posible modificarlo, sustituir los valores correspondiente de cada variable y cambiarle el nombre a ".env".
  
  ![Estructura del archivo .env](images/Captura_env.jpg)
  
* Crear las siguientes carpetas a la misma altura del `docker-compose.yaml`.

```bash
mkdir -p dags,logs,plugins,data
```
* Ejecutar el siguiente comando para levantar los servicios de Airflow.

```bash
docker-compose up --build
```
* Una vez que los servicios estén levantados, ingresar a Airflow en `http://localhost:8080/`.

* En la pestaña `Admin -> Connections` crear una nueva conexión con los siguientes datos para Redshift:

    * Conn Id: `redshift_default`
    * Conn Type: `Amazon Redshift`
    * Host: `host de redshift`
    * Database: `base de datos de redshift`
    * Schema: `esquema de redshift`
    * User: `usuario de redshift`
    * Password: `contraseña de redshift`
    * Port: `5439`
  
 En la pestaña `Admin -> Variables` crear las siguientes variables de entorno para el funcionamient de la API y para envio de correo automatico:

    * APIKEY: `key de la api news-Api.org`
    * TOP_HEADLINES_URL: `https://newsapi.org/v2/everything`
    * SMTP_EMAIL_FROM : `Correo del remitente`
    * SMTP_EMAIL_TO : `Correo del Destinatario`
    * SMTP_HOST: `Host smtp`
    * SMTP_PASSWORD: `Contraseña del smtp`
    * SMTP_PORT: `Puerto del smtp`

Los comprobantes de las notificaciones se encuentran en la carpeta "images".


