# Usar una imagen base de Python
FROM python:3.9-slim

# Establecer el directorio de trabajo dentro del contenedor
WORKDIR /app

# Instalar Apache Airflow y otras dependencias necesarias
RUN pip install --no-cache-dir apache-airflow==2.4.0 requests psycopg2-binary python-dotenv

# Copiar los archivos necesarios al contenedor
COPY Primera_PreEntrega_Batista.py /app/
COPY .env /app/
COPY dags/ /usr/local/airflow/dags/

# Configuración del entorno de Airflow
ENV AIRFLOW_HOME=/usr/local/airflow

# Inicializar la base de datos de Airflow
RUN airflow db init

# Comando para iniciar el scheduler de Airflow
CMD ["airflow", "scheduler"]
