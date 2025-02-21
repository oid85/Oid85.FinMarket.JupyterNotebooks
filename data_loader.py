import psycopg2 as ps
import pandas as pd
import config


def get_candles_by_ticker(ticker):
    connection = ps.connect(host = config.host, port = config.port, database = config.database, user = config.user, password = config.password)
    
    sql = f"select date, open, close, high, low, volume from storage.daily_candles where instrument_id in (select instrument_id from public.instruments where ticker = '{ticker}') and date >= '{config.start_date}' and date <= '{config.end_date}' order by date"
    df = pd.read_sql(sql, con = connection)
    
    return df


def get_analyse_results_by_ticker(ticker, analyse_type):
    connection = ps.connect(host = config.host, port = config.port, database = config.database, user = config.user, password = config.password)
    
    sql = f"select date, result_string, result_number from storage.analyse_results where instrument_id in (select instrument_id from public.instruments where ticker = '{ticker}') and date >= '{config.start_date}' and date <= '{config.end_date}' and analyse_type = '{analyse_type}' order by date"
    df = pd.read_sql(sql, con = connection)
    
    return df


def get_close_prices():
    df = pd.DataFrame(columns=config.watch_list_shares)

    for ticker in config.watch_list_shares:
        df_candles = get_candles_by_ticker(ticker)
        df[ticker] = df_candles["close"]
    
    return df


def get_normalized_close_prices():
    df = pd.DataFrame(columns=config.watch_list_shares)

    for ticker in config.watch_list_shares:
        df_candles = get_candles_by_ticker(ticker)
        df[ticker] = df_candles["close"] / df_candles["close"][0]
    
    return df


def get_ltm():
    df = pd.DataFrame(columns=config.watch_list_shares)

    for ticker in config.watch_list_shares:
        df_analyse_results = get_analyse_results_by_ticker(ticker, "YieldLtm")
        df[ticker] = df_analyse_results["result_number"]
    
    return df