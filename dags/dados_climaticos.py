from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
import pendulum
import pandas as pd
from airflow.macros import ds_add

with DAG(
    "dados_climaticos",
    start_date = pendulum.datetime(2024,3,11, tz='UTC'),
    schedule_interval='0 0 * * 1', # executar toda segunda feira
) as dag:
    create_folder = BashOperator(
        task_id='cria_pasta',
        bash_command = 'mkdir -p "/mnt/c/Users/gabri/Documents/data-pipeline-airflow/week={{data_interval_end.strftime("%Y-%m-%d")}}"'
    )

    def extrai_dados(data_interval_end):
        city = 'Sao%20Paulo'
        key = 'xxxxxxxxxxxxx'

        url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{city}/{data_interval_end}/{ds_add(data_interval_end, 7)}?unitGroup=metric&include=days&key={key}&contentType=csv"

        df = pd.read_csv(url)

        file_path = f"/mnt/c/Users/gabri/Documents/data-pipeline-airflow/week={data_interval_end}/"

        df.to_csv(file_path + 'dados_brutos.csv')
        df[['datetime','tempmin','temp','tempmax']].to_csv(file_path + 'temperaturas.csv')
        df[['datetime','description','icon']].to_csv(file_path + 'condicoes.csv')

    extrai_dados = PythonOperator(
        task_id = 'extrai_dados',
        python_callable = extrai_dados,
        op_kwargs = {'data_interval_end': '{{data_interval_end.strftime("%Y-%m-%d")}}'}
    )

    create_folder >> extrai_dados

