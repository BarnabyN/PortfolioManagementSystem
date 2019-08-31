import pandas as pd
from alpha_vantage.timeseries import TimeSeries
import xlwings as xw
import datetime as dt
import quandl
from modData_base import data_source
    


# Alphavantage data, less stable so we use the quandl data where we can
class AV_data():
    
    dctAssetMap = {'SPY':{'name':'SPDR S&P 500 ETF','asset_class':'US Equity'},
                   'IXIC':{'name':'Nasdaq Index','asset_class':'US Equity'},
                   '^VUKE':{'name':'Vanguard FTSE 100 ETF','asset_class':'UK Equity'},
                   'GSG':{'name':'iShares S&P GSCI Commodity','asset_class':'Commodities'},
                   'AAXJ':{'name':'iShares MSCI Asia ex Japan ETF','asset_class':'Asia ex Japan Equity'},
                   'IWM':{'name':'iShares Russell 2000 ETF','asset_class':'US Small Cap Equity'},
                   'EWJ':{'name':'iShares MSCI Japan ETF', 'asset_class':'Japan Equity'},
                   'FEZ':{'name':'SPDR Eurostoxx 50 ETF', 'asset_class':'Europe ex UK Equity'},
                   '^VMID':{'name':'Vanguard FTSE 250 ETF', 'asset_class':'UK Mid Cap Equity'},
                   'MCHI':{'name':'iShares MSCI China ETF', 'asset_class':'China Equity'},
                   'EEM':{'name':'iShares MSCI Emerging Markets ETF', 'asset_class':'Emerging Market Equity'}}


    def string_to_date(self, date):
        date = dt.datetime.strptime(date, '%Y-%m-%d')
        return date
    
    def fix_type(self, d):
        if d==str(d):
            return self.string_to_date(d)
        else:
            return d
    
    def daily_close(self, ticker, start_date, end_date):

        av = TimeSeries(key='SJA94IVRELN62B8S', output_format='pandas')
        srs = av.get_daily(symbol=ticker, outputsize='full')[0]
        
        srs.index = [dt.datetime.strptime(i, '%Y-%m-%d').date() for i in srs.index]
    
        start_date = self.fix_type(start_date)
        end_date = self.fix_type(end_date)
        
        # Accounting for weekends and holidays, moves date forward
        while start_date not in srs.index:
            start_date = start_date + dt.timedelta(days=1)
        while end_date not in srs.index:
            end_date = end_date + dt.timedelta(days=1)
            
        srs = srs[start_date:end_date]['4. close']
        srs.name = ticker
        return srs   

    
    def find_series(self, keyword):
        res = requests.get(url='https://www.alphavantage.co/query?function=SYMBOL_SEARCH&keywords={}&apikey=SJA94IVRELN62B8S&datatype=csv'.format(keyword))
        txt = res.text.split('\r\n')
        txt = pd.DataFrame([i.split(',') for i in txt])
        txt.columns = txt.iloc[0]
        txt = txt[1:]
        return txt

alphavantage_data = AV_data()
