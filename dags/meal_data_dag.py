from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os
import requests
import psycopg2
from dotenv import load_dotenv

def execute_script():
    load_dotenv('.env')

    api_url = "https://www.themealdb.com/api/json/v1/1/search.php?s=Arrabiata"
    response = requests.get(api_url)
    data = response.json()

    meals = data['meals']
    transformed_data = []
    for meal in meals:
        transformed_data.append((
            meal['idMeal'],
            meal['strMeal'],
            meal['strCategory'],
            meal['strArea'],
            meal['strInstructions'],
            meal['strMealThumb'],
            meal['strTags'],
            meal['strYoutube']
        ))

    conn = psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASS'),
        dbname='data-engineer-database'
    )
    cur = conn.cursor()

    try:
        create_table_query = """
        CREATE TABLE IF NOT EXISTS meal_data (
            idMeal VARCHAR(50),
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

        insert_query = """
        INSERT INTO meal_data (idMeal, strMeal, strCategory, strArea, strInstructions, strMealThumb, strTags, strYoutube)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        for record in transformed_data:
            try:
                cur.execute(insert_query, record)
            except Exception as e:
                print(f"Error inserting record {record}: {e}")
                conn.rollback()

        conn.commit()

    except Exception as e:
        print(f"Error creating table or inserting data: {e}")
        conn.rollback()
    finally:
        cur.close()
        conn.close()

    print("Proceso completado")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG('meal_data_ingestion', default_args=default_args, schedule_interval='@daily') as dag:
    run_script = PythonOperator(
        task_id='execute_script',
        python_callable=execute_script
    )
