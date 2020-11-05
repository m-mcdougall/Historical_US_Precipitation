# -*- coding: utf-8 -*-


#%%

'''
Plots ideas

'''


'''
PLot for each hour of the day with a stacked barplot, each colour representing a season
That'll show the hourly prevalence of rain by season, and the total amount of rain for that hour

'''


"""

Duration and volume of rain events would provide better metrics for making predictions. 
I would like to look at how the rain intensity(rain volume/duration),
 total volume and frequency has changed over the 70 years of data, 
 and use that to make observations and predictions for future weather conditions. 
 This would reflect climate change's effect on weather patterns on a local level.


"""
#%%

import os
import pandas as pd
from tqdm import tqdm
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import time
import concurrent.futures as cf


wd=os.path.abspath('C://Users//Mariko//Documents//GitHub//The-5-PM-rains')
os.chdir(wd)

#Import the saved csv
df_md=pd.read_csv('Cleaned_data_MD.csv', index_col=0, parse_dates=['Date'] )

#df_md=pd.read_csv('Cleaned_data_FL.csv', index_col=0, parse_dates=['Date'] )
#%%

def gen_hours():
    am=['12AM']+[str(i)+'AM' for i in range(1,12)]
    pm=['12PM']+[str(i)+'PM' for i in range(1,12)]
    return am+pm

#%%


def simple_annual_sums_line(df_in, print_legend=False):
    
    plt.figure(figsize=(11,4))
    
    for station in df_in.Station.unique():
        
        sat_ann=df_in[df_in.Station==station].sort_values('Date')
        
        
        annual_hourly=sat_ann.groupby(by='Year').sum()
        annual_sum=annual_hourly.sum(axis=1).reset_index()
    
    
        plt.plot(annual_sum.Year, annual_sum.iloc[:,1], label=station)

    #Add the plot formatting
    plt.title('Annual Precipitation for each Station in MD')
    plt.ylabel('Precipitation in Inches')

    if print_legend == True:
        plt.legend()

simple_annual_sums_line(df_md)

"""
We need to remove non-consecutive stations
and make note of how many stations are present each year
"""

#%%

def filter_nonconsecutive_years(df_in, max_years_skipped=0, print_warnings=False):
    
    def check_consecutive(list_in, max_years_skipped):
        perfect_list=list(range(min(list_in),max(list_in)+1))
       
        if perfect_list==list_in:
            return 1
        
        else:
            if len(perfect_list)-len(list_in)<=max_years_skipped:
                return 1
            else:                    
                return 0
    
    
    station_list=[]

    for station in df_in.Station.unique():
        
        station_years=df_in[df_in.Station==station].Year.unique().tolist()
        if not check_consecutive(station_years, max_years_skipped):
            station_list.append(station)
            
            if print_warnings==True:
                print(f'\nRemoving: {station} \n{station_years}\n----------')
                
    
    df_in=df_in[~df_in.Station.isin(station_list)]
    
 
    return df_in
        
        



df_consecutive=filter_nonconsecutive_years(df_md, max_years_skipped=0, print_warnings=True)

simple_annual_sums_line(df_consecutive)
simple_annual_sums_line(df_md)

#%%

def plot_num_stations(df_in):
        
    station_count=df_in.groupby("Year").Station.nunique()
    
    plt.figure(figsize=(12,3))
    plt.xlim(station_count.index.min()-2,station_count.index.max()+2)
    plt.ylim(0, station_count.max()+2)
    plt.bar(station_count.index, station_count)
    
    plt.yticks()
    plt.title('Number of Stations Reporting in MD')
    plt.ylabel('# Unique Stations')


plot_num_stations(df_md)
plot_num_stations(df_consecutive)
#%%%
    

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



#%%
    
#Filter for minimum number of years open, then see how the plots look
x=filter_min_station_duration(df_consecutive, min_days=365*45)
min_10_years=filter_min_station_duration(df_consecutive, min_days=365*10)

simple_annual_sums_line(x)
plot_num_stations(x)

simple_annual_sums_line(min_10_years)
plot_num_stations(min_10_years)

#%%



#%%



def rainfall_metrics_calc(df_in):
    """
    Takes the hourlty station data and returns the ranfall metrics:Volume, Duration and intensity for each occurance
    """

    
    
    all_rain=df_in.iloc[:,5:].copy()
    tracking=df_in.iloc[:,0:5].copy()
    
    #Need to insert a row at the start to account for rain at the starting hour - otherwise the boolenas get flipped
    all_rain.insert(loc=0, column='Start', value=0)
    
    #Binary rain-no rain
    all_rain_binary=all_rain!=0
    
    #Make a copy, then shift columns by one
    all_rain_binary_shift=all_rain_binary.copy()
    
    cols=all_rain.columns.to_list()
    cols=cols[1:]+[cols[0]]
    all_rain_binary_shift=all_rain_binary_shift[cols]
    
    #Set to arrays, then subtract the shifted array from the true array
    #The result has the start and stop indexes for each rainfall.
    #The -1 index is the last zero before the start(Don't count!), the 1 is an end (Count!)
    rain_times=np.array(all_rain_binary).astype(int)-np.array(all_rain_binary_shift).astype(int)
    
    #Counts the number of times a rainfall occured
    num_rainfalls=rain_times==-1
    num_rainfalls=num_rainfalls.sum(axis=1)
    
    
    
    #Seperate out the times where it rained only once
    #Can be processesd faster
    
    
    one_rain=all_rain[num_rainfalls==1]
    
    one_rain_metrics=tracking[num_rainfalls==1].copy()
    one_rain_metrics["Volume"]=one_rain.sum(axis=1)
    one_rain_metrics["Duration"]=(one_rain>0).sum(axis=1)
    
    
    
    #Seperate out the days that rained multiple times
    multi_rain=all_rain[num_rainfalls>1]
    multi_rain_tracking=tracking[num_rainfalls>1]
    multi_rain_binary=rain_times[num_rainfalls>1]
    
    #Obtain the start and stop indexes for all rainfall events
    row_start, col_start=np.where(multi_rain_binary == -1)
    row_stop, col_stop=np.where(multi_rain_binary == 1)
    
    #Shift the indexes by 1, since the 1st should be non-inclusive, and the last should be inclusive
    col_start=col_start+1 
    col_stop=col_stop+1
    
    
    
    
    
    def metrics_calculator3(i):
        """
        Takes the range shape[0] to loop through
        """
        event=multi_rain.iloc[row_start[i], col_start[i]:col_stop[i]]
        tracking_info=multi_rain_tracking.iloc[row_start[i], :].values
        
        out=np.append(tracking_info,[event.sum(), event.shape[0]])
        metrics.append(out)
        
    
    metrics=[]   
    start=time.perf_counter()
    
    #Using process pool as it is ~3x faster than threaded 
    with cf.ThreadPoolExecutor() as executor:
    #with cf.ProcessPoolExecutor() as executor:
        executor.map(metrics_calculator3, range(row_start.shape[0]))#map calls a function for each list element that is passed
    
    
        
    metrics=pd.DataFrame(metrics)
    metrics.columns=tracking.columns.to_list()+['Volume','Duration']
    
    
    finish=time.perf_counter()
    
    print(f'Metrics finished in {round(finish-start,2)} seconds')
    
    #Glue on the single rainfalls, and re-sort
    
    metrics=pd.concat([metrics, one_rain_metrics], ignore_index=True)
    metrics=metrics.sort_values(['Station','Date']).reset_index(drop=True)
    metrics["Intensity"]=metrics["Volume"]/metrics["Duration"]
    
    return metrics



x_metrics=rainfall_metrics_calc(x)

min_10_years_metrics=rainfall_metrics_calc(min_10_years)
#%%



#Plot average with std by year - all stations merged



def seperate_seasons(df_in):
    """
    We'll use the meteorological season starts, rather than the astronomical starts
    This means that we use rounded-off months starting on the 1st
    """
    
    seasons= ['Winter', 'Spring', 'Summer','Fall']
    seasons = [ele for ele in seasons for i in range(3)] 
    
    dates=[i for i in range(1,12+1)]
    dates=dates[-1:]+dates[:11]
    
    dates=dict(zip(dates,seasons))
    
    df_in=df_in.copy()
    df_in["Season"]=df_in.Month.replace(dates)
    
    season_colours={'Winter':'#486BF9', 'Spring':'#48F97E', 'Summer':'#F9D648','Fall':'#F96648'}
    
    return df_in, season_colours



def plot_seasonal_metrics_line(df_in, column="Volume", stations_in='all', confidince='sd'):
    
    
    df_in=df_in.copy()
    
    if stations_in.lower() != 'all':
        try:
            df_in=df_in[df_in.Station==stations_in]
        except:
            raise ValueError("That station is not present, please try again.")
    
    
    df_in=seperate_seasons(df_in)[0]
    
    
    
    
    plt.figure(figsize=(12,3))
    
    
    
    
    sns.lineplot( data=df_in, hue='Season',x='Year', y=column,ci=confidince)
    plt.show()
    




for i in ["Duration" ,"Volume" ,"Intensity" ]:
    plot_seasonal_metrics_line(x_metrics, i )
    plot_seasonal_metrics_line(x_metrics, i, confidince=None )



#%%


def plot_seasonal_metrics_stack(df_in, column="Volume"):
    """
    Plot the Military expenditure as a stacked plot, to get a feel of proportional spending
    """

    df_in=df_in.copy()
    

    
    df_in, colours=seperate_seasons(df_in)
    
    df_in2=df_in.filter([column, "Season", "Year"], axis=1)
    
    bottom=np.zeros(len(df_in2.Year.unique()))
    
    plt.figure(figsize=(11,4))
    for season in df_in.Season.unique():
        
        df_season=df_in2[df_in2.Season==season]
        df_season=df_season.groupby('Year').mean()
        
        plt.bar(df_season.index,df_season[column] , bottom=bottom, color=colours[season], label=season)
        bottom=df_season[column]+bottom

    
    plt.title(f'Variations in Seasonal {column} over Time')
    plt.ylabel(column)
    plt.legend(bbox_to_anchor=(1.0, .7))


for i in ["Duration" ,"Volume" ,"Intensity" ]:
    plot_seasonal_metrics_stack(x_metrics, column=i)


#%%


#Let's look at individual sations changes in reported variation to see if individul stations are experiencing more change than others
#Average over the 1st 5 years and last 5 years to smooth out annual variation




def stack_deltas(df_in, column="Intensity", season_interest='All', mode='All'):
    
    df_in=df_in.copy()
    
    #Add a season column to the dataframe, drop extra columns
    df_in, colours=seperate_seasons(df_in)
    df_in=df_in.filter(["Station", 'Year',  "Season", column], axis=1)
    
    
    #Determine the span of years where all stations can be covered
    if mode.lower()=='all':
        #Get the minimum of the max years and the max of the min year
        max_year=df_in.groupby("Station").Year.max().min()
        min_year=df_in.groupby("Station").Year.min().max()
    
    #Prepare a dataframe to capture the deltas
    df_plot=pd.DataFrame(columns=["Station",  "Season", 'Delta'])
    
    
    def extract_append_delta(df_extract, season_str,min_year, max_year):
        """
        Extract the mean value from the first 5 years, and the final 5 years, calculate the difference
        Return the delta values as a series, to be appended to the df_plot
        """
        start_val=df_extract[(df_extract.Year>=min_year) & (df_extract.Year<=min_year+5)][column].mean()
        end_val=df_extract[(df_extract.Year>=max_year-5) & (df_extract.Year<=max_year)][column].mean()
        return pd.Series({"Station":station,  "Season":season_str, 'Delta':end_val-start_val})
    
    
            
    for station in df_in.Station.unique():
        df_station=df_in[df_in.Station==station]
        
        if mode.lower()=='individual':
            min_year=df_station.Year.min()
            max_year=df_station.Year.max()
            
        if season_interest.lower()=="combined":
            df_plot=df_plot.append(extract_append_delta(df_station, season_interest,min_year, max_year), ignore_index=True)
    
        else:
            for season in df_station.Season.unique():
                df_season=df_station[df_station.Season==season]
                
                df_plot=df_plot.append(extract_append_delta(df_season, season,min_year, max_year), ignore_index=True)
           
    
    
    fig, ax= plt.subplots(figsize=(11,4))  
    
    #If not extracting seasonal data, plot a simple barplot with the deltas
    if season_interest.lower()=="combined":
        plt.bar(df_plot.Station, df_plot.Delta)
    
    #If extracting seasonal data, proceed.
    else:
        #For the seasonal stacked plots, need to seperate positive and negative deltas to stack properly
        #Prepare dictionaries to capture positive and negative bottom values 
        positive={station:0 for station in df_plot.Station.unique() }
        negative={station:0 for station in df_plot.Station.unique() }
        
                
        for season in df_in.Season.unique():
            #Isolate the season 
            df_season=df_plot[df_plot.Season==season]
            df_season.index=df_season.Station
            
            #Capture the values for the bottom values in each season's plot 
            bottom=[]
            for station in df_season.Station.unique():
                val=df_season.loc[station, 'Delta']
                
                #Determine if delta is +/-, then extract and update correct bottoms
                if val>0:
                    bottom.append(positive[station])
                    positive[station]+=val
                else:
                    bottom.append(negative[station])
                    negative[station]+=val
                
            #Plot that season
            plt.bar(df_season.Station, df_season.Delta, bottom=bottom, color=colours[season], label=season)
    
    #Set titles and legend based on input params        
    if mode.lower()=='individual':
        title_string=f'Changes in Average {column}\n First 5 Years per Station vs Final 5 Years per Station'
    else:
        title_string=f'Changes in Average {column}\n {min_year}-{min_year+5} vs {max_year-5}-{max_year}'

    if season_interest.lower()=="all":
        plt.legend( bbox_to_anchor=(1.0, .9))


    #Set the rest of the plot params
    ax.axhline(y=0, color='k')
    
    plt.ylabel(f'Change in Average {column}')
    plt.xticks( rotation='vertical', size=6)
    plt.title(title_string)



#stack_deltas(x_metrics, column="Volume", season_interest='All', mode='All')
#stack_deltas(x_metrics, column="Volume", season_interest='All', mode='Individual')
#stack_deltas(x_metrics, column="Volume", season_interest='Combined', mode='All')
#stack_deltas(x_metrics, column="Volume", season_interest='Combined', mode='Individual')


for i in ["Duration" ,"Volume" ,"Intensity" ]:
    stack_deltas(x_metrics, column=i, season_interest='All', mode='All')
    #stack_deltas(x_metrics, column=i, season_interest='Combined', mode='All')
    
    
for i in ["Duration" ,"Volume" ,"Intensity" ]:
    stack_deltas(x_metrics, column=i, season_interest='All', mode='Individual')
    #stack_deltas(x_metrics, column=i, season_interest='Combined', mode='Individual')
#%%











































