import json
from datetime import datetime
import pandas as pd
import requests
from bs4 import BeautifulSoup
from pipelines.data_cleaning import clean_text

def get_wikipedia_page(url):
    try:
        response= requests.get(url,timeout=10)
        response.raise_for_status()
        return response.text

    except requests.RequestException as e:
        print(f"Error Occured: {e}")

def get_wikipedia_data(html):
    soup = BeautifulSoup(html, 'html.parser')
    table=soup.find_all("table")[2]
    rows=table.find_all('tr')
    return rows

def extract_data(**kwargs):
    url = kwargs['url']
    html = get_wikipedia_page(url)
    extracted_rows = get_wikipedia_data(html)

    data = []
    for rows in range(1,len(extracted_rows)):
        tds=extracted_rows[rows].find_all('td')
        # rank=rows,
        # stadium=tds[0].text,
        # capacity=tds[1].text,
        # region=tds[2].text,
        # country=tds[3].text,
        # city=tds[4].text,
        # images=tds[5].find('img').get('src').split("//")[1] if tds[5].find('img') else "No Image",
        # home_teams=tds[6].text
        #
        # s=Sports(rank, stadium, capacity, region, country, city, images, home_teams)
        # data.append(s)
        values = {
            'rank': rows,
            'stadium': clean_text(tds[0].text),
            'capacity': clean_text(tds[1].text),
            'region': clean_text(tds[2].text),
            'country': clean_text(tds[3].text),
            'city': clean_text(tds[4].text),
            'images': 'https://' + tds[5].find('img').get('src').split("//")[1] if tds[5].find('img') else "No Image",
            'home_team': clean_text(tds[6].text),
        }
        data.append(values)

    json_data=json.dumps(data)
    kwargs['ti'].xcom_push(key='rows',value=json_data)
    return "Data extracted successfully!"

def transform_data(**kwargs):
    data=kwargs['ti'].xcom_pull(key='rows', task_ids='extract_data_from_wikipedia')
    data=json.loads(data)

    df=pd.DataFrame(data)
    df['capacity']=df['capacity'].str.replace(',','').astype(int)
    df['stadium'].drop_duplicates()
    kwargs['ti'].xcom_push(key='rows',value=df.to_json())
    return "Data transformed successfully!"

def write_data(**kwargs):
    data=kwargs['ti'].xcom_pull(key='rows', task_ids='transform_data_from_wikipedia')
    data=json.loads(data)
    data=pd.DataFrame(data)

    date=datetime.now().strftime('%Y-%m-%d')
    file=f"stadium_data_{date}.csv"

    filesystem_name = "sportsdata" #container name
    account_name = "sportsanalyticsdata" #storage account
    file_name = f"initial_data/{file}"
    account_key = "y8CDeR40vDA9vEN2LGB0qG5n6YMQ/J5CWEZ7DsnqekNuafpr6Di5sJaTv2cag2aVgQYcczUh2p+e+ASt8XqeqA=="

    abfs_path = f"abfs://{filesystem_name}@{account_name}.dfs.core.windows.net/{file_name}"

    data.to_csv(
        abfs_path,
        storage_options={'account_key': account_key},
        index=False
    )























