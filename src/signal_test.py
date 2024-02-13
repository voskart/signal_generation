from db_connection import MongoConnection
from pymongo.errors import ConnectionFailure, CollectionInvalid
import datetime as dt
from datetime import timedelta
import polars as pl
import os
import time
from scipy import stats
from dotenv import load_dotenv
from velodata import lib as velo
from backtest import Backtest

class Signal():
    
    db = None

    def __init__(self) -> None:
        try:
            load_dotenv()
            mc = MongoConnection()
            self.db = mc.client['velodata']
        except ConnectionFailure as e:
            raise e

    def get_futures_data(self, exchange='binance-futures', start_date=dt.datetime(2024, 1, 1), end_date=dt.datetime.now()):
        try:
            return self.db.futures.find({
                'exchange': exchange
            })
        except CollectionInvalid as e:
            raise e
    
    def get_options_data(self):
        client = velo.client(os.environ.get('VELO_API'))
        print(client.get_term_structure(coins=['BTC']))

    def get_signals(self, group):
        window_size = 240
        step = 120
        start = 0
        end = start + window_size
        max_increase_tau = {'tau': 0, 'range_low':0, 'range_high':0, 'oi_difference':0}
        max_decrease_tau = {'tau': 0, 'range_low':0, 'range_high':0, 'oi_difference':0}
        # calcuate tau, calculate OI increase for those with positive tau, calculate funding and premium avg for the periods
        rows = []
        iterations = 1
        while end < len(group):
            df_cut = group[start:end].with_row_index()
            tau_oi = stats.kendalltau(df_cut['index'], df_cut['dollar_open_interest_close'])
            tau_price = stats.kendalltau(df_cut['index'], df_cut['close_price'])
            if tau_oi.statistic < 0:
                if max_decrease_tau['tau'] > tau_oi.statistic:
                    max_decrease_tau = {'tau': tau_oi.statistic, 'range_low': start, 'range_high': end}
            else:
                max_increase_tau = {'tau': tau_oi.statistic, 'range_low': start, 'range_high': end}
                # get mean premium for period
                premium = df_cut['premium'].mean()
                # get mean funding for period
                funding = df_cut['funding_rate'].mean()
                # OI increase
                oi_increase = df_cut[-1]['dollar_open_interest_close']-df_cut[1]['dollar_open_interest_close']
                rows.append({"Currency": group['product'][0], "Start_Date": group[max_increase_tau['range_low']].select(pl.col('time')).item(), "End_Date": group[max_increase_tau['range_high']].select(pl.col('time')).item(), "TAU_OI": tau_oi.statistic, "TAU_Price": tau_price.statistic, "Funding": funding, "Premium": premium})
                # print(group[max_increase_tau['range_low']].select(pl.col('time')).item(), group[max_increase_tau['range_high']].select(pl.col('time')).item(), max_increase_tau['range_low'], max_increase_tau['range_high'])
            start += step
            end += step
        return(pl.DataFrame(rows))
    
    def identify_signals(self, tau_oi=0.7, price_oi=0.7):
        # get minute data
        start = time.time()
        df = pl.read_parquet(f"./data/futures_data_{dt.date.today()}.parquet")
        df = df.filter(pl.col('search_resolution')==1)
        df = df.unique(subset='time')
        df = df.sort(by='time')
        # filter out bnb
        df = df.filter(pl.col('product')!='BNBUSDT')
        df_processed = df.group_by('product').apply(function=self.get_signals)
        # filter for long signals, price constant, oi increase, funding negative
        df_up = df_processed.filter((pl.col('TAU_OI')>0.6) & ((pl.col('Funding')<-0.0001) | (pl.col('Premium')<0)))
        df_up = df_up.with_columns(pl.lit('up').alias('Signal'))
        # filter for short signals, price constant, oi increase, funding negative
        df_down = df_processed.filter((pl.col('TAU_OI')>0.6) & ((pl.col('Funding')>0.0001) | (pl.col('Premium')>0)))
        df_down = df_down.with_columns(pl.lit('down').alias('Signal'))
        end = time.time()
        # combine dfs
        df_combined = pl.concat([df_up, df_down])
        print(f"{df.group_by('product').count()}")
        print(f"Range from: {df.sort(by='time')['time'][0]} to : {df.sort(by='time')['time'][-1]}")
        print(f"Number of signals: {len(df_up)}")
        print(f"Elapsed time to run analysis: {end - start}")
        return df_combined, df
    
def main():
    sig = Signal()
    if not os.path.exists(f"./data/futures_data_{dt.date.today()}.parquet"):
        futures_data = list(sig.get_futures_data())
        df = pl.DataFrame(futures_data)
        # god knows where this column is coming from (probably mongo db id that we don't care about but it kills the script)
        df = df.drop("_id")
        df.write_parquet(f"./data/futures_data_{dt.date.today()}.parquet")
    else:   
        df_signals, df_data = sig.identify_signals()
        # backtest = Backtest(df_signals, df_data)
        # df = backtest.get_returns()
        # filter latest signals, i.e., the ones that are 48h or fresher
        df_signals_newest = df_signals.filter(pl.col('End_Date')>dt.datetime.now()-timedelta(hours=48))
        # send latest signal per currency
        latest_signal_by_coin = df_signals_newest.group_by(pl.col('Currency')).agg(pl.all().sort_by('End_Date').last())
        # write signals to db
        client = MongoConnection(False)
        client.insert_signals(latest_signal_by_coin)

if __name__ == '__main__':
    main()