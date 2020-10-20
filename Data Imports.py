# -*- coding: utf-8 -*-
"""
Created on Sat Oct 17 10:38:16 2020

@author: Mariko
"""
#%%

import os
import pandas as pd

wd=os.path.abspath('C://Users//Mariko//Documents//GitHub//The-5-PM-rains')
os.chdir(wd)

os.listdir()
#%%

#Import the files from csv and concatinate 

df_1950=pd.read_csv('1951-1970.csv')
df_1970=pd.read_csv('1970-1990.csv')
df_1990=pd.read_csv('1990-2014.csv')


df_raw=pd.concat([df_1950, df_1970,df_1990], ignore_index=True )

#These dataframes are fairly laarge, so delete when finishe importing
del df_1950, df_1970, df_1990

#Look at our data
df_raw.head(15)

#We have some cleaning to do:
#Delete the Station#, reformat the date to be easily parsed, remove the 999.99 vals,
# and check for duplicate entries

#%%

