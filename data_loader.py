import psycopg2 as ps
import pandas as pd
import config


def get_prices():
    connection = ps.connect(
        host     = config.host, 
        port     = config.port, 
        database = config.database, 
        user     = config.user, 
        password = config.password)
    
    df = pd.DataFrame(columns=config.watch_list_shares)

    for ticker in config.watch_list_shares:    
        df_candles = pd.read_sql("select date, close from storage.daily_candles where instrument_id in (select instrument_id from public.instruments where ticker = '" + ticker + "') and date >= '" + config.start_date + "' and date <= '" + config.end_date + "' order by date", con = connection)
        df[ticker] = df_candles["close"]
    
    return df


def get_normalized_prices():
    connection = ps.connect(
        host     = config.host, 
        port     = config.port, 
        database = config.database, 
        user     = config.user, 
        password = config.password)
    
    df = pd.DataFrame(columns=config.watch_list_shares)

    for ticker in config.watch_list_shares:    
        df_candles = pd.read_sql("select date, close from storage.daily_candles where instrument_id in (select instrument_id from public.instruments where ticker = '" + ticker + "') and date >= '" + config.start_date + "' and date <= '" + config.end_date + "' order by date", con = connection)
        df[ticker] = df_candles["close"] / df_candles["close"][0]
    
    return df