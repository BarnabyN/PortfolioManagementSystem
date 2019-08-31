import pandas as pd
import xlwings as xw
import datetime as dt
import quandl
import numpy as np
import pickle

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

    AllAssets = [
    ['US Equity', 'CHRIS/CME_SP1', 'S&P500 F1'],
    ['UK Equity', 'CHRIS/CME_Z1', 'FTSE100 F1'],
    ['Commodities', 'CHRIS/SHFE_AU2', 'Gold F2'],
    ['Europe Equity', 'CHRIS/EUREX_FESX1', 'Euro Stoxx 50 F1'],
    ['India Equity', 'CHRIS/SGX_IN1', 'Nifty 50 F1'],
    ['EM Equity', 'CHRIS/EUREX_FMFM1', 'MSCI Frontier Markets F1'],
    ['EM Equity', 'CHRIS/EUREX_FMEM1', 'MSCI EM F1'],
    ['EM Equity', 'CHRIS/EUREX_FMMX1', 'MSCI Mexico F1'],
    ['US Equity', 'CHRIS/ICE_RV1', 'Russell 1000 Value F1'],
    ['Sentiment', 'AAII/AAII_SENTIMENT', 'AAII Data'],
    ['US Government Bonds', 'CHRIS/CME_TY1', 'US 10 Year F1'],
    ['US Rates', 'USTREASURY/YIELD', 'US Yield Curve'],
    ['UK Rates', 'BOE/IUDMNPY', 'UK 10 Year Yield'],
    ['US Rates', 'FED/TIPSY', 'US TIPS Yields']
    ]
    
    def __init__(self, debug=False):
        self.debug = debug
        self.asset_classes = [i[0] for i in self.AllAssets]
        self.asset_codes = [i[1] for i in self.AllAssets]
        self.asset_names = [i[2] for i in self.AllAssets]
        # df is the data downloaded from quandl, is assigned later in code
        self.df = None 
        self.loaded = False

    def documentation(self):
        with open('Documentation.txt', 'r') as f:
            return f.read()

    def _debug(self, msg):
        if self.debug:
            print('Debug ==> ', msg)

    def _attribute_working_asset_info(self):
        self.working_asset_names = [self.asset_names[self.asset_codes.index(i)] for i in self.working_asset_codes]
        self.working_asset_classes = [self.asset_classes[self.asset_codes.index(i)] for i in self.working_asset_codes]

    def time_series(self, ticker, start_date, end_date):
        df = quandl.get(ticker)
        df.index = [pd.to_datetime(i) for i in df.index]
        
        # Process start date if non-empty
        if start_date != '':
            start_date = self.fix_type(start_date)
            while start_date not in df.index:
                start_date = start_date + dt.timedelta(days=1)

        # Proess end date if non-empty
        if end_date != '':
            end_date = self.fix_type(end_date)
            while end_date not in df.index:
                end_date = end_date + dt.timedelta(days=1) 

        # If the user leaves the start or end date as an empty string '', it goes from the start/end of the data
        if start_date != '' and end_date != '':
            df = df[start_date:end_date]
        elif start_date == '' and end_date != '':
            df = df[:end_date]
        elif start_date != '' and end_date == '':
            df = df[start_date:]
        else:
            df = df
        # Set name of df equal to name associated with ticker you input
        df.name = self.asset_names[self.asset_codes.index(ticker)]
        return df

    def raw_bulk_query(self, tickers, start_date, end_date):
        # Pass tickers as a list of codes, even if len(tickers)==1
        self.working_asset_codes = []
        working_asset_data = []
        for tick in tickers:
            # Get time series of the ticker
            srs = self.time_series(tick, start_date, end_date) # This will break if no data is retreivable
            working_asset_data.append(srs[srs.columns[0]])
            # If the data is retreived, add ticker to the working list
            self.working_asset_codes.append(tick)
            self._debug('Data retreived: {}'.format(self.asset_names[self.asset_codes.index(tick)]))
        # Save the working assets as attributes
        self._attribute_working_asset_info()
        df = pd.concat(working_asset_data, axis=1)
        return df

    def _attribute_multiindex(self):
        # Create a 3 level pandas multi-index
        arrays = [self.working_asset_classes, self.working_asset_codes, self.working_asset_names]
        level_names = ['Asset Class', 'Code', 'Name']
        index = pd.MultiIndex.from_arrays(arrays=arrays, names=level_names)
        # Create as attribute for easy acces when loading data
        self.columns_multiindex = index
        
        return self.columns_multiindex

    def download_data(self, save=True):
        df = self.raw_bulk_query(tickers=self.asset_codes, start_date='2000-01-31', end_date='')
        # Attribute the row and col indices
        self.rows_dateindex = df.index
        df.columns = self._attribute_multiindex()
        self.df = df
        # Overwrite the pickle file (True by default)
        if save:
            self._save_data_to_file(file_name='pickleData.pkl')
        self.loaded = True
        return df

    def _save_data_to_file(self, file_name):
        with open(file_name, 'wb') as file:
                pickle.dump(self.df, file)

    def load_data(self):
        with open('pickleData.pkl', 'rb') as file:
                df = pickle.load(file)
        df = pd.DataFrame(df)
        if self.loaded:
            df.columns = self.columns_multiindex
        self.df = df
        self.loaded = True
        return df
