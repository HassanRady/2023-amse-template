import pandas as pd

def make_unique(df: pd.DataFrame):
    df['id'] = df['STATIONS_ID'].astype(str) + df['MESS_DATUM'].astype(str)
    return df.drop_duplicates(subset='id', keep='first', ignore_index=True)

