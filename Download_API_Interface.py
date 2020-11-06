# -*- coding: utf-8 -*-

import os
import pandas as pd
from tqdm import tqdm

wd=os.path.abspath('C://Users//Mariko//Documents//GitHub//The-5-PM-rains')
os.chdir(wd)


#%%


station_loc=pd.read_fwf('MASTER-STN-HIST.txt', sep=' ')

station_loc.columns=(range(station_loc.shape[1]))

#Filter out non-US weather stations
station_loc=station_loc[station_loc[6]=='STATES']

df_station=station_loc.filter([1,7,10,12,13,14,15,16,17,18,19])

df_station.columns=["Station_ID","State", "Station",
                     "Start_Date","End_Date",
                     "Lat_Deg","Lat_Min","Lat_S",
                     "Long_Deg","Long_Min","Long_S",]


z=df_station.copy()

#%%
df_station=z.copy()

#Replace error codes
df_station=df_station.replace(to_replace=10101, value=99999999999, method=None)




df_processing=df_station.groupby('Station').head(1).copy()
df_processing.index=df_processing.Station

df_processing=df_processing[(df_processing.Station_ID.str.len()>10) & 
                            (df_processing.Station_ID.str.count(" ")<3)]

df_processing["Station_ID"]=df_processing["Station_ID"].apply(lambda x : x.split(' ')[1])

df_processing=df_processing[(df_processing.Start_Date!=99999999999)]


df_filter=df_processing.groupby('Station_ID').head(1).copy()
df_filter.index=df_filter.Station_ID
df_filter["Start_Date"]=df_processing.groupby('Station_ID').Start_Date.min()
df_filter["End_Date"]=df_processing.groupby('Station_ID').End_Date.max()



df_filter=df_filter.dropna().reset_index(drop=True)


#%%

df_downloader=df_processing.copy()

df_downloader[df_downloader.State=='AK']










































