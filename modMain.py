from modData import QuandlData
import pandas as pd
import numpy as np
import pickle


# How to save dataframe while maintaining multi index

foo = QuandlData()

df = foo.download_data()

