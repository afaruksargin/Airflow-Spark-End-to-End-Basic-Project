import requests
import pandas as pd
import json
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook




def main():
    
    url =  "https://data.cityofnewyork.us/resource/k397-673e.json"

    response = requests.get(url)

    if response.status_code==200:
        data = response.json()
        df = pd.DataFrame(data)
        #df.to_csv("/home/user/Masaüstü/PythonÖrnekÇalışmalar/Airflow-Spark-Data-Pipline/datasets/ham_veri.csv",index = False)

    create_sql_script = "CREATE TABLE IF NOT EXISTS staging.example (fiscal_year int NULL , title_description varchar(200) NULL, regular_hours float NULL, regular_gross_paid float NULL) ;"

    return df[["fiscal_year","title_description","regular_hours","regular_gross_paid"]] , create_sql_script

        

if __name__ ==  '__main__':
    main()
    