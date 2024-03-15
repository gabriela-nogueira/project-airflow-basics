import os
from os.path import join
import pandas as pd
from datetime import datetime, timedelta

# intervalo de datas
start_date = datetime.today()
end_date = start_date + timedelta(days=7)

# formatando as datas
start_date = start_date.strftime("%Y-%m-%d")
end_date = end_date.strftime("%Y-%m-%d")

city = 'Sao%20Paulo'
key = 'xxxxxxxxxxxxxxxxxx'

url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{city}/{start_date}/{end_date}?unitGroup=metric&include=days&key={key}&contentType=csv"

df = pd.read_csv(url)

print(df.head())

file_path = f"/mnt/c/Users/gabri/Documents/data-pipeline-airflow/week={start_date}/"

os.mkdir(file_path)

df.to_csv(file_path + 'dados_brutos.csv')
df[['datetime','tempmin','temp','tempmax']].to_csv(file_path + 'temperaturas.csv')
df[['datetime','description','icon']].to_csv(file_path + 'condicoes.csv')


