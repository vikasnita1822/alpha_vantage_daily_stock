from tqdm import tqdm
import pandas as pd
from psycopg2 import connect
import json
import requests
from datetime import datetime, timedelta

file_path = 'creds_template.json'
with open(file_path,'r') as f:
    config_data = json.load()

db_host = config_data['POSTGRES_HOST']
db_port = config_data['POSTGRES_PORT']
db_name = config_data['POSTGRES_DATABASE']
db_user = config_data['POSTGRES_USERNAME']
db_password = config_data['POSTGRES_PASSWORD']
       
conn = connect(
        database=db_name,
        user=db_user,
        password=db_password,
        host=db_host,
        port = db_port
    )

API_KEY = config_data['API_KEY']
COMPANIES = config_data['COMPANIES']
start_date = config_data["start_date"]
end_date = config_data["end_date"]


def fetch_stock_data(company,API_KEY):
    url = f'http://www.alphavantage.co/query?function=TIME_SERIES_DAILY_ADJUSTED&symbol={company}&outputsize=full&apikey={API_KEY}'
    response = requests.get(url)
    data = response.json()
    return data['Time Series (Daily)']


def required_date_range_tuple(COMPANIES):
    yesterday = (datetime.now() - timedelta(1)).strftime('%Y-%m-%d')
    final = []
    for company in tqdm(COMPANIES):
        for date, values in data.items():
            try:
                data = fetch_stock_data(company)
                if yesterday <= date:
                    final.append((
                        date,
                        company,
                        float(values['1. open']),
                        float(values['4. close']),
                        float(values['2. high']),
                        float(values['3. low']),
                        int(values['6. volume'])
                    ))
            except Exception as e:
                print(e)

    return final

def insert_into_database(conn, data_tuple):
    cur = conn.cursor()

    insert_string = '''INSERT INTO stock_prices(date, company, open, close, high, low, volume) VALUES (%s, %s, %s, %s, %s, %s, %s)'''

    batch_size = 20000
    for i in tqdm(range(0, len(data_tuple), batch_size)):
        batch = data_tuple[i:i + batch_size]
        if batch:
            cur.executemany(insert_string, batch)

    conn.commit()






