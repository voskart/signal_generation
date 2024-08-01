import polars as pl
import datetime as dt
from datetime import timedelta

class Backtest():
    def __init__(self, signals: pl.DataFrame, data: pl.DataFrame) -> None:
        self.signals = signals
        self.data = data

    # get end date for each row, get price 12hr from signal
    def get_returns(self):
        # for each signal check price right after end_date
        return(self.signals.apply(function=self.get_price))

    def get_price(self, df, hours=[4, 8, 12, 24, 48]):
        price = self.data.filter((pl.col('product') == df[0]) & (pl.col('time')>=df[2]))["close_price"]
        print(f"Currency: {df[0]}, from: {df[1]} to {df[2]}, Signal: {df[7]}")
        prices = []
        for h in hours:
            newtime = df[2]+timedelta(hours=h)
            newprice = self.data.filter((pl.col('product') == df[0]) & (pl.col('time')>=newtime))["close_price"]
            if (newtime > dt.datetime.now()) | (not len(newprice)):
                print("Time would be in the future!")
                break
            print(f"Price right at signal time: {price[0]}, price {h} hours after: {newprice[0]}, price change: {1-price[0]/newprice[0]:2f}")
            prices.append(newprice[0])       
        return(f"Initial: {price[0]}", prices)

def main():
    pass

if __name__ == "__main__":
    main()