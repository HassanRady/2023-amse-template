from urllib.request import urlretrieve
import zipfile

import pandas as pd
import sqlalchemy

url = "https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip"

r = urlretrieve(url, "exercises/mowesta.zip")

with zipfile.ZipFile("exercises/mowesta.zip") as z:
    z.extractall("exercises/mowesta")

df = pd.read_csv("exercises/mowesta/data.csv", sep=';' , header=None, names=range(454))

df = df.dropna(axis=1)
df_cleaned = df.drop(columns=[ 12, 13])
df_cleaned.columns = df_cleaned.iloc[0, :]
df_cleaned = df_cleaned.drop(index=0)
df_cleaned = df_cleaned[["Geraet", "Hersteller", "Model", "Monat", "Temperatur in °C (DWD)", "Batterietemperatur in °C", "Geraet aktiv"]]

df_transformed = df_cleaned.rename({"Temperatur in °C (DWD)":"Temperatur", "Batterietemperatur in °C":"Batterietemperatur"}, axis=1)

for col in ["Temperatur", "Batterietemperatur"]:
    df_transformed[col] = df_transformed[col].replace(",", ".", regex=True)
    df_transformed[col] = df_transformed[col].astype(float)

df_transformed['Monat'] = df_transformed['Monat'].astype(int)
df_transformed['Geraet'] = df_transformed['Geraet'].astype(int)

def to_fahrenheit(x):
    return x * 9/5 + 32

df_transformed['Temperatur'] = df_transformed['Temperatur'].apply(to_fahrenheit)
df_transformed['Batterietemperatur'] = df_transformed['Batterietemperatur'].apply(to_fahrenheit)

df_transformed.to_sql("temperatures", "sqlite:///temperatures.sqlite", index=False)