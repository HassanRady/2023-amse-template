import os

from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
from data.config import config
import pandas as pd
import requests, zipfile, io

from database_client import SqliteClient

def get_raw_data_to_sqlite():

    req = Request(config.weather_data.WEATHER_DATA_URL)
    html_page = urlopen(req)

    soup = BeautifulSoup(html_page, "html")

    links = []
    for link in soup.findAll('a'):
        links.append(link.get('href'))

    target_links = []

    for link_name in links:
        if ".zip" in link_name:
            target_links.append(link_name)


    for zip_link in target_links:
        r = requests.get(config.weather_data.WEATHER_DATA_URL+zip_link)
        with zipfile.ZipFile(io.BytesIO(r.content)) as z:
            z.extractall(config.weather_data.WEATHER_DATA_PATH)


    target_names = []

    for file_name in os.listdir(config.weather_data.WEATHER_DATA_PATH):
        if "produkt" in file_name:
            target_names.append(file_name)

    df = pd.DataFrame()
    for name in target_names:
        df = pd.concat([df, pd.read_csv(config.weather_data.WEATHER_DATA_PATH + name, sep=";")], axis=0)

    df.to_sql("raw_weather_data", SqliteClient.db_engine)
