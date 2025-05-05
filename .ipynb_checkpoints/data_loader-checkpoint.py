import psycopg2 as ps
import pandas as pd
import config


def get_daily_candles_by_ticker(ticker):
    connection = ps.connect(host = config.host, port = config.port, database = config.database, user = config.user, password = config.password)    
    sql = f"select date, open, close, high, low, volume from storage.daily_candles where instrument_id in (select instrument_id from public.instruments where ticker = '{ticker}') and date >= '{config.start_date}' and date <= '{config.end_date}' order by date"
    df = pd.read_sql(sql, con = connection)    
    
    return df

def get_five_minute_candles_by_ticker(ticker):
    connection = ps.connect(host = config.host, port = config.port, database = config.database, user = config.user, password = config.password)    
    sql = f"select date, time, open, close, high, low, volume from storage.five_minute_candles where instrument_id in (select instrument_id from public.instruments where ticker = '{ticker}') and date >= '{config.start_date}' and date <= '{config.end_date}' order by datetime"
    df = pd.read_sql(sql, con = connection)    
    
    return df

def get_analyse_results_by_ticker(ticker, analyse_type):
    connection = ps.connect(host = config.host, port = config.port, database = config.database, user = config.user, password = config.password)    
    sql = f"select date, result_string, result_number from storage.analyse_results where instrument_id in (select instrument_id from public.instruments where ticker = '{ticker}') and date >= '{config.start_date}' and date <= '{config.end_date}' and analyse_type = '{analyse_type}' order by date"
    df = pd.read_sql(sql, con = connection)    
    
    return df


def get_daily_close_prices():
    watch_list_shares = config.get_watch_list_shares()
    df = pd.DataFrame(columns=watch_list_shares)
    
    for ticker in watch_list_shares:
        df_candles = get_daily_candles_by_ticker(ticker)

        if len(df_candles) > 0:
            df[ticker] = df_candles["close"]
    
    return df


def get_normalized_daily_close_prices():
    watch_list_shares = config.get_watch_list_shares()
    df = pd.DataFrame(columns=watch_list_shares)
    
    for ticker in watch_list_shares:
        df_candles = get_daily_candles_by_ticker(ticker)

        if len(df_candles) > 0:
            df[ticker] = df_candles["close"] / df_candles["close"][0]
    
    return df


def get_imoex_ltm():
    ticker = 'IMOEX'
    df = pd.DataFrame(columns=[ticker])
    df_analyse_results = get_analyse_results_by_ticker(ticker, "YieldLtm")
    df[ticker] = df_analyse_results["result_number"]
    
    return df