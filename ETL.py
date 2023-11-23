from datetime import datetime
import os
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.postgres_operator import PostgresOperator  


def extract_and_transform(**kwargs):
    try:
        # Construire le chemin complet vers le fichier CSV
        script_directory = os.path.dirname(os.path.abspath(__file__))
        data_directory = os.path.join(script_directory, 'projet', 'data')
        csv_file_path = os.path.join(data_directory, 'donnees-urgences-SOS-medecins.csv')

        # Charger les données
        df_urgences = pd.read_csv(csv_file_path)

        # Supprimer les lignes avec des valeurs manquantes
        df_urgences = df_urgences.dropna()

        # Supprimer les lignes en double
        df_urgences = df_urgences.drop_duplicates()

        # Vérifier les données manquantes
        print(df_urgences.isnull().sum())

        # Afficher le DataFrame
        print(df_urgences)

        # Sauvegarder le DataFrame nettoyé dans un nouveau fichier CSV
        df_urgences.to_csv('donnees_transformees.csv', index=False)

    except Exception as e:
        # Imprimer l'erreur pour le débogage
        print(f"An error occurred: {e}")
        # Lever à nouveau l'exception pour indiquer que la tâche a échoué
        raise
def data_transform_and_load(**kwargs):
    try:
        # Connexion à la base de données PostgreSQL
        conn_id = 'table_connexion'  # Assurez-vous que c'est la bonne connexion
        engine = create_engine(conn_id)

        # Charger les données transformées depuis le fichier CSV
        df_transformed = pd.read_csv('donnees_transformees.csv')

        # Exécuter des opérations de transformation supplémentaires si nécessaire
        # ...

        # Charger les données dans la base de données PostgreSQL
        df_transformed.to_sql('nom_de_la_table', con=engine, index=False, if_exists='replace')

    except Exception as e:
        print(f"An error occurred: {e}")
        raise

# Définir le DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False
}
dag = DAG(
    'ETL_Urgences',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
)

# Tâche pour effectuer l'extraction et la transformation
etl_task = PythonOperator(
    task_id='Extract_Transform_Load',
    python_callable=extract_and_transform,
    provide_context=True,
    dag=dag,
)
create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='postgres_connexion',
    sql='C:\\Users\\Anis\\Desktop\\Projet DaW\\dagscreate_table.sql'
)
transform_and_load = PythonOperator(
    task_id = 'transform_and_load',
    python_callable = data_transform_and_load
    )

etl_task >> create_table >> transform_and_load

