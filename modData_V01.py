import pandas as pd
from alpha_vantage.timeseries import TimeSeries
import xlwings as xw
import datetime as dt
import quandl

class data_source:
    
    def string_to_date(self, date):
        date = dt.datetime.strptime(date, '%Y-%m-%d')
        return date
    
    def fix_type(self, d):
        if d==str(d):
            return self.string_to_date(d)
        else:
            return d

class QuandlData(data_source):
    
    # Good to have this class here as it allows me to override things in all modules below
    
    def time_series(self, ticker, start_date, end_date):
        
        df = quandl.get(ticker)
        df.index = [pd.to_datetime(i) for i in df.index]
        start_date = self.fix_type(start_date)
        end_date = self.fix_type(end_date)
        # Accounting for weekends and holidays, moves date forward
        while start_date not in df.index:
            start_date = start_date + dt.timedelta(days=1)
        while end_date not in df.index:
            end_date = end_date + dt.timedelta(days=1) 
        df = df[start_date:end_date]
        df.name = [i[1] for i in self.series_list if i[0]==ticker][0]
        return df
    
    def documentation(self):
        with open('Documentation.txt', 'r') as f:
            return f.read()
    
class EquityPrices(QuandlData):
    
    def __init__(self):
        self.series_list = [['CHRIS/CME_SP1', 'S&P500 F1', 'US Equity'],
                            ['CHRIS/CME_Z1', 'FTSE100 F1', 'UK Equity'],
                            ['CHRIS/SHFE_AU2', 'Gold F2', 'Commodities'],
                            ['CHRIS/EUREX_FESX1', 'Euro Stoxx 50 F1', 'Europe Equity'],
                            ['CHRIS/SGX_IN1', 'Nifty 50 F1', 'India Equity'],
                            ['CHRIS/EUREX_FMFM1', 'MSCI Frontier Markets F1', 'EM Equity'],
                            ['CHRIS/EUREX_FMEM1', 'MSCI EM F1', 'EM Equity'],
                            ['CHRIS/EUREX_FMMX1', 'MSCI Mexico F1', 'EM Equity'],
                            ['CHRIS/ICE_RV1', 'Russell 1000 Value F1', 'US Equity']]
        self.codes = [i[0] for i in self.series_list]
        self.names = [i[1] for i in self.series_list]
        
    
    def bulk_query(self, tickers, start_date, end_date):
        # Pass tickers as a list of codes, even if len(tickers)==1
        df = pd.concat([self.time_series(i, start_date, end_date)['Settle'] for i in tickers], axis=1)
        df.columns = [i[1] for i,j in zip(self.series_list, tickers) if j==i[0]] # Make columns equal to names
        return df


class SentimentData(QuandlData):
    
    def __init__(self):
        self.series_list = [['AAII/AAII_SENTIMENT', 'AAII Data', 'Sentiment']]
        self.codes = [i[0] for i in self.series_list]
        self.names = [i[1] for i in self.series_list]

    def aaii_indicator(self):
        return self.time_series(ticker=self.codes[0], start_date='2018-01-01', end_date='2019-04-01')
        

class GovernmentBondYields(QuandlData):

    def __init__(self):
        self.series_list = [['CHRIS/CME_TY1', 'US 10 Year F1', 'US Government Bonds'],
                            ['USTREASURY/YIELD', 'US Yield Curve', 'US Rates'],
                            ['BOE/IUDMNPY', 'UK 10 Year Yield', 'UK Rates'],
                            ['FED/TIPSY', 'US TIPS Yields', 'US Rates']]
        self.codes = [i[0] for i in self.series_list]
        self.names = [i[1] for i in self.series_list]


def get_all_data():
    lstAllData = []
    for assetType in [EquityPrices, GovernmentBondYields]:
        assetType_ = assetType()
        for i in assetType_.codes:
            try:
                lstAllData.append(assetType_.time_series(ticker=i, start_date='1960-12-31', end_date='2019-04-30'))
            except:
                print(i, ' did not work')
    df = pd.concat(lstAllData, axis=1)
    return df

