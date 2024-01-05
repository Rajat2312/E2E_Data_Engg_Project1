import psycopg2
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow import DAG
from datetime import datetime
import subprocess

# Install dependencies
def install_dependencies():
    subprocess.run(['pip','install','numpy'])
    subprocess.run(['pip','install','psycopg2'])
    # subprocess.run(['pip','install','numpy'])
    # return 1

# Connect to PostgreSQL
def connect_to_db():
    # db = winedb
    conn = psycopg2.connect(database="postgres",user="user",password="Password@9876",
                            host="postgres-sql-db",port="5432")
    conn.close()
    print('DB connected successfully')

def create_table_db():
    # conn = psycopg2.connect(database="postgres",user="user",password="Password@9876",
    #                         host="postgres-sql-db",port="5432")
    # conn.autocommit = True
    # cursor = conn.cursor()
    # create_table = '''CREATE TABLE wine_data 
    #                 (Alcohol float, Malic_Acid float, Ash float,
    #                 Ash_Alcanity float, Magnesium int, Total_Phenols float,
    #                 Flavanoids float, Nonflavanoid_Phenols float, Proanthocyanins float,
    #                 Color_Intensity float, Hue float, OD280 float, Proline int,
    #                 Customer_Segment int, id int);
    #                 '''
    # cursor.execute(create_table)
    # conn.close()
    print('Table created successfully')

def copy_csv_to_table():
    conn = psycopg2.connect(database='postgres',user='user',password='Password@9876',
                            host='postgres-sql-db',port='5432')
    conn.autocommit = True
    cursor = conn.cursor()
    copy_csv = '''
               COPY wine_data (Alcohol, Malic_Acid, Ash, Ash_Alcanity, Magnesium, Total_Phenols,
               Flavanoids, Nonflavanoid_Phenols, Proanthocyanins, Color_Intensity, Hue,
               OD280, Proline, Customer_Segment, id)
               FROM '/opt/airflow/dags/wine_cleaned.csv'
               DELIMITER ','
               CSV HEADER;
               '''
    cursor.execute(copy_csv)
    conn.close()

# Define the DAG
data_ingestion_dag = DAG(dag_id='data-ingestion',
                         description='Data Ingestion DAG from CSV to PostgreSQL DB',
                         schedule_interval='@daily',
                         start_date=datetime(2024,1,4))

# Define the tasks
task0 = PythonOperator(task_id='Install-dependencies',
                       python_callable=install_dependencies,
                       dag=data_ingestion_dag)
task1 = BashOperator(task_id='data-cleaning',
                     bash_command='python '+'/opt/airflow/dags/data_cleaning.py',
                     dag=data_ingestion_dag)
task2 = PythonOperator(task_id='Connect-to-pgdb',
                       python_callable=connect_to_db,
                       dag=data_ingestion_dag)
task3 = PythonOperator(task_id='Create-Table',
                       python_callable=create_table_db,
                       dag=data_ingestion_dag)
task4 = PythonOperator(task_id='Copy-csv-to-table',
                       python_callable=copy_csv_to_table,
                       dag=data_ingestion_dag)

task0>>task1>>task2>>task3>>task4
    
print('Done')