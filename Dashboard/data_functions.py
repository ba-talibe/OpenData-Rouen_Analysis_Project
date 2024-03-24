import pandas as pd

def process_data(df):
    df['Time'] = pd.to_datetime(df['Date_JJMMAA HH:MM'].str.split('+').str[0],format="%Y-%m-%dT%H:%M:%S")
    df['Date'] = pd.to_datetime(df['Date_JJMMAA'])
    df['Year'] = df['Date'].dt.year
    df['Month'] = df['Date'].dt.month
    df['DayOfWeek'] = df['Date'].dt.day_of_week
    df['Hour'] = df['Time'].dt.hour
    df['Month-Year'] = pd.to_datetime(df['Month'].astype(str)+'-'+df['Year'].astype(str))
    
def counter_list(df):
    return sorted(df['Nom'].unique())