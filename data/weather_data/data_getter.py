import os

from bs4 import BeautifulSoup
from urllib.request import Request, urlopen
from data.config import config
import pandas as pd
import requests, zipfile, io

from database_client import SqliteClient

def get_links_from_page(url):
    req = Request(url)
    html_page = urlopen(req)

    soup = BeautifulSoup(html_page, "html")

    links = []
    for link in soup.findAll('a'):
        links.append(link.get('href'))
    return links

def get_station_description(name):
    if name is None:
        return
    tmp = []
    data = urlopen(config.weather_data.WEATHER_DATA_URL+name)
    for n, line in enumerate(data): 
        if n == 0 or n == 1:
            tmp.append(line)
            continue
        tmp.append(line.decode("iso-8859-1").split(r"  "))

    tmp2 = []
    for line in tmp[2:]:
        line = list(map(lambda x: x.strip(), line))
        line = list(filter(None, line))
        tmp2.append(line)

    rows = []
    for i, line in enumerate(tmp2):
        tmp4 = []
        for j, v in enumerate(line):
            if j == 0:
                for w in v.split():
                    tmp4.append(w)
                continue
            if j == 3:
                import re
                match = re.match(r"([0-9]+.[0-9]+)", v, re.I)
                if match:
                    items = match.groups()
                tmp4.append(items[0])
                tmp4.append(v[len(items[0]):])
                continue
            tmp4.append(v)
        rows.append(tmp4)

        header = [h.decode("utf-8") for h in tmp[0].split()]
    station_description_df = pd.DataFrame(data=rows, columns=header)
    station_description_df.to_sql("weather_station_description", SqliteClient.db_engine, index=False, if_exists='replace')

def get_link_content(link):
    r = requests.get(config.weather_data.WEATHER_DATA_URL+link)
    return r.content

def extract_zip_file(file):
    with zipfile.ZipFile(io.BytesIO(file)) as z:
        z.extractall(config.weather_data.WEATHER_DATA_PATH)

def read_weather_files(dir):
    target_names = []

    for file_name in os.listdir(dir):
        if "produkt" in file_name:
            target_names.append(file_name)

    df = pd.DataFrame()
    for name in target_names:
        df = pd.concat([df, pd.read_csv(config.weather_data.WEATHER_DATA_PATH + name, sep=";")], axis=0)
    return df


def get_raw_data_to_sqlite():

    links = get_links_from_page(config.weather_data.WEATHER_DATA_URL)

    target_links = []

    for link_name in links:
        if ".zip" in link_name:
            target_links.append(link_name)
        if "KL_Tageswerte_Beschreibung_Stationen" in link_name:
            station_description_link = link_name

    get_station_description(station_description_link)

    for zip_link in target_links:
        content = get_link_content(zip_link)
        extract_zip_file(content)

    df = read_weather_files(config.weather_data.WEATHER_DATA_PATH)
    # TODO: make station id and date composite key and handle duplication insertion
    df.to_sql("raw_weather_data", SqliteClient.db_engine, index=False, if_exists='replace')
