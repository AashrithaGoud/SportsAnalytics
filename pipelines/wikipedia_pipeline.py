import requests
from bs4 import BeautifulSoup
import re
# url="https://en.wikipedia.org/wiki/List_of_association_football_stadiums_by_capacity"
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
    url=kwargs['url']
    html=get_wikipedia_page(url)
    data=get_wikipedia_data(html)

    print(data)











