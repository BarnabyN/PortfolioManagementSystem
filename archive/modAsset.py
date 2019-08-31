import pandas as pd
import numpy as np
import modData


class asset:
    
    def __init__(self, ticker):
        self.ticker = ticker
        self.asset_class = None
        
    def _import_raw_data(self, ticker, start, end):
        df = quandl_data.futures_prices(ticker, start, end)
        return df
    
    def returns(self, start_date, end_date, freq='D'):
        df = self._import_raw_data(self.ticker, start_date, end_date)

        if freq == 'D':
            df = df/df.shift(1)-1
            df = df[1:].fillna(value=0)
        
        elif freq == 'W':
            df = df.resample('W').last()
            df = df/df.shift(1)-1
            df = df.fillna(value=0)
        
        elif freq == 'M':
            df = df.resample('M').last()
            df = df/df.shift(1)-1
            df = df.fillna(value=0)
        
        return pd.DataFrame(df)


    

    


    







	

	

	
	
