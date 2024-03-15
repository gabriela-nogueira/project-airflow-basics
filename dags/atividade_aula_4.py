from airflow.models import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

def cumprimentos():
    print("Bem vindo ao Airflow!")

with DAG(
    dag_id = 'atividade_aula_4',
    start_date = days_ago(1),
    schedule_interval = '@daily'
) as dag:
    tarefa_1 = PythonOperator(
        task_id = 'atividade_aula_4',
        python_callable = cumprimentos
    )

    tarefa_1