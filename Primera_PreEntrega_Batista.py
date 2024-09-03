import os
import subprocess
from dotenv import load_dotenv
import requests
import psycopg2
import json

# Instalar las dependencias necesarias
subprocess.run(['pip', 'install', 'python-dotenv'])
subprocess.run(['pip', 'install', 'requests', 'psycopg2-binary'])

# Crear el archivo .env con las credenciales de la base de datos
with open('.env', 'w') as f:
    f.write("""
DB_HOST=data-engineer-cluster.cyhh5bfevlmn.us-east-1.redshift.amazonaws.com
DB_PORT=5439
DB_USER=bf_miguel_coderhouse
DB_PASS=HW767tUWkj
DB_NAME=dev
    """)

# Verificar el contenido del archivo .env
with open('.env', 'r') as f:
    print(f.read())

# Cargar las variables de entorno
load_dotenv('.env')

# Verificar que las variables se han cargado correctamente
print(f"DB_HOST: {os.getenv('DB_HOST')}")
print(f"DB_USER: {os.getenv('DB_USER')}")

# Realizar la solicitud a la API
api_url = "https://www.themealdb.com/api/json/v1/1/search.php?s=Arrabiata"
response = requests.get(api_url)

if response.status_code == 200:
    data = response.json()
    meals = data.get('meals', [])

    if meals:
        transformed_data = []
        for meal in meals:
            transformed_data.append((
                meal.get('idMeal'),
                meal.get('strMeal'),
                meal.get('strCategory'),
                meal.get('strArea'),
                meal.get('strInstructions'),
                meal.get('strMealThumb'),
                meal.get('strTags'),
                meal.get('strYoutube')
            ))

        # Conectar a la base de datos y ejecutar la consulta
        try:
            conn = psycopg2.connect(
                host=os.getenv('DB_HOST'),
                port=os.getenv('DB_PORT'),
                user=os.getenv('DB_USER'),
                password=os.getenv('DB_PASS'),
                dbname=os.getenv('DB_NAME')
            )
            cur = conn.cursor()

            # Crear la tabla en Redshift si no existe
            create_table_query = """
            CREATE TABLE IF NOT EXISTS meal_data (
                idMeal TEXT,
                strMeal TEXT,
                strCategory TEXT,
                strArea TEXT,
                strInstructions TEXT,
                strMealThumb TEXT,
                strTags TEXT,
                strYoutube TEXT,
                ingestion_time TIMESTAMP DEFAULT GETDATE()
            );
            """
            cur.execute(create_table_query)
            conn.commit()
            print("Tabla 'meal_data' creada o verificada exitosamente.")

            # Insertar los datos en la tabla
            insert_query = """
            INSERT INTO meal_data (
                idMeal, 
                strMeal, 
                strCategory, 
                strArea, 
                strInstructions, 
                strMealThumb, 
                strTags, 
                strYoutube
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            for record in transformed_data:
                try:
                    cur.execute(insert_query, record)
                except Exception as e:
                    print(f"Error al insertar el registro {record}: {e}")
                    conn.rollback()
                else:
                    conn.commit()
            print("Datos insertados correctamente en la tabla 'meal_data'.")

        except Exception as e:
            print(f"Error al conectar o interactuar con la base de datos: {e}")
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()
            print("Conexión a la base de datos cerrada.")
    else:
        print("No se encontraron datos en la respuesta de la API.")
else:
    print(f"Error al realizar la solicitud a la API. Código de estado: {response.status_code}")

print("Proceso completado.")
