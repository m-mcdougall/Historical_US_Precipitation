# -*- coding: utf-8 -*-

#%%

import os
import pandas as pd
from tqdm import tqdm

wd=os.path.abspath('C://Users//Mariko//Documents//GitHub//The-5-PM-rains')
os.chdir(wd)


#%%

state='Maryland'
state='Florida'

#Import the files from csv and concatinate 

all_dfs=[pd.read_csv('./'+state+'/'+file) for file in os.listdir('./'+state)]

df_raw=pd.concat(all_dfs, ignore_index=True )

#These dataframes are fairly large, so delete when finished importing
del all_dfs

#Look at our data
df_raw.head(15)
original_shape=df_raw.shape

#We have some cleaning to do:
#Delete the Station#, reformat the date to be easily parsed, remove the 999.99 vals,
# and check for duplicate entries

#%%

#Drop the Station ID
df_raw.drop('STATION', axis=1, inplace=True)

#Rename Cols to better names
df_raw=df_raw.rename({'STATION_NAME':'Station', "HPCP":"Volume", 'DATE':'Date'}, axis=1)


df_raw.head(15)
#%%

def check_duplicates(df_in, print_out=False):
    """
    Checks for duplicate datetimes in the dataframe, prints any duplicate rows and keeps the first instance.
    These duplicates are caused by January 1st being represented in both sides of the joined datasets
    
    """
    
    unique_times=[]
    
    for station in df_in.Station.unique():
        
        #Sort one station's readings at a time 
        df_one=df_in[df_in.Station==station]
        
        #Check if there are duplicated rows
        if df_one.shape[0]!=df_one.Date.nunique():
            
            if print_out==True:
                #Prints any duplicated rows, with the matching times together
                print('   -------------------------------------------')
                print()
                print(df_one[df_one.Date.duplicated(keep=False)].sort_values('Date'))
                print()
            
            #Keep only the first duplicated value
            df_one=df_one[~df_one.Date.duplicated()].reset_index(drop=True)
            
        unique_times.append(df_one)
            
    #Once looped through, concat the contents of the unique list
    output=pd.concat(unique_times).reset_index(drop=True)
    
    print (f'\n             ------------\n\
           Operation Complete\n\
           {int(df_in.shape[0]-output.shape[0])} Duplicates Removed')
    print (f'             ------------')
    
    return output



df_raw=check_duplicates(df_raw, print_out=False)




#%%
df_raw.head(15)


#Drop the missing values

df_raw=df_raw[df_raw.Volume != 999.99]

df_raw.head(15)


#%%


#Change the Date column to a date, seperate out values


df_raw=df_raw.rename(columns={'Date':'Date_str'})

df_raw['Date_Full']=pd.to_datetime(df_raw.Date_str)
df_raw['Date']=df_raw.Date_Full.dt.date
df_raw['Hour']=df_raw.Date_Full.dt.hour


print(df_raw.head())
print(df_raw.tail())

df_raw.drop('Date_str', axis=1, inplace=True)

#%%


print(df_raw.head())


#%%

def filter_min_station_duration(df_in, min_days=365):
    """
    Remove stations that were in operation for less than a specified number of days, measured from first observation to last observation.. 
    Default value = 365 (1 year)
    """
    
    stations_to_drop=[]
    
    for station in df_in.Station.unique():
        daily=df_in[(df_in.Station==station)]
        
        first_day=daily.Date.iloc[0]
        last_day=daily.Date.iloc[-1]
        
        delta=(last_day-first_day).days
        if delta<min_days:
            print(f'{station} - Open for {delta} days.')
            stations_to_drop.append(station)
            
    output=df_in[~df_in.Station.isin(stations_to_drop)]
            
    return output


df_raw=filter_min_station_duration(df_raw, min_days=365)


#%%

def gen_hours():
    am=['12AM']+[str(i)+'AM' for i in range(1,12)]
    pm=['12PM']+[str(i)+'PM' for i in range(1,12)]
    return am+pm


#%%

def reshape_time(df_in):
    
    #These are the 24h in the day, to be column labels
    hours=gen_hours()
    
    #Catch the reformated dataframes
    pivoted_frames=[]
    
    for station in tqdm(df_in.Station.unique()):
        try:
            
            #Filter fir the station
            daily=df_in[(df_in.Station==station)]
    
            #Pivot the hours into columns
            day_pivot=daily.pivot(index='Date', columns='Hour', values=['Volume'])
    
            #Rename/label to be useful
            day_pivot.columns=hours
            day_pivot.fillna(0, inplace=True)
            day_pivot.reset_index(inplace=True)
            day_pivot['Station']=station
            
            #Reorder the columns
            cols=day_pivot.columns.to_list()
            cols=cols[-1:]+cols[:-1]
            
            day_pivot=day_pivot[cols]
            
            pivoted_frames.append(day_pivot)
    
        except:
            print(f'\nERROR: Please check {station}')
            
    output=pd.concat(pivoted_frames, ignore_index=True).reset_index(drop=True)
    
    return output


#%%

df_raw=reshape_time(df_raw)
df_raw.head(20)
#%%





#%%

def add_time_indexes(df_in):
    """
    Add some columns to the dataframe to make parsing the dates easier
    """
    
    #Add the new columns
    df_in['Year']=pd.DatetimeIndex(df_in['Date']).year
    df_in['Month']=pd.DatetimeIndex(df_in['Date']).month
    df_in['Day']=pd.DatetimeIndex(df_in['Date']).day
    
    #Reorder the columns
    cols=df_in.columns.to_list()
    cols=cols[0:2]+cols[-3:]+cols[2:-3]
    df_in=df_in[cols]


    return df_in

#%%
df_raw=add_time_indexes(df_raw)






#%%

print(f'The original shape of the data was {original_shape}')
print(f'The cleaned shape of the data was {df_raw.shape}')

state=df_raw.iloc[0,0]
state=state[state.find(' US')-2 : state.find(' US')]


df_raw.to_csv('Cleaned_data_'+state+'.csv')






