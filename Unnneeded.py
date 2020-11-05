# -*- coding: utf-8 -*-
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
    
    
    return df_in



#%%%
    

"""
Metrics counter versions
These versions were not as fast as version 3, 
Version 1 relies on all submitting simultaniously which was often NOT THE CASE.


"""




import time
import concurrent.futures as cf



#two_rain=x.iloc[10:14,5:].copy()
#tracking=x.iloc[10:14,0:5].copy()


two_rain=x.iloc[:,5:].copy()
tracking=x.iloc[:,0:5].copy()

#Need to insert a row at the start to account for rain at the starting hour - otherwise the boolenas get flipped
two_rain.insert(loc=0, column='Start', value=0)

#Binary rain-no rain
two_rain_binary=two_rain!=0

#Make a copy, then shift columns by one
two_rain_binary_shift=two_rain_binary.copy()

cols=two_rain.columns.to_list()
cols=cols[1:]+[cols[0]]
two_rain_binary_shift=two_rain_binary_shift[cols]

#Set to arrays, then subtract the shifted array from the true array
#The result has the start and stop indexes for each rainfall.
#The -1 index is the last zero before the start(Don't count!), the 1 is an end (Count!)
rain_times=np.array(two_rain_binary).astype(int)-np.array(two_rain_binary_shift).astype(int)

#Counts the number of times a rainfall occured
num_rainfalls=rain_times==-1
num_rainfalls=num_rainfalls.sum(axis=1)



#Obtain the start and stop indexes for all rainfall events
row_start, col_start=np.where(rain_times == -1)
row_stop, col_stop=np.where(rain_times == 1)

#Shift the indexes by 1, since the 1st should be non-inclusive, and the last should be inclusive
col_start=col_start+1 
col_stop=col_stop+1





def metrics_calculator3(i):
    """
    Takes the range shape[0] to loop through
    """
    event=two_rain.iloc[row_start[i], col_start[i]:col_stop[i]]
    tracking_info=tracking.iloc[row_start[i], :].values
    
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
metrics["Intensity"]=metrics["Volume"]/metrics["Duration"]

finish=time.perf_counter()

print(f'Metrics 3 finished in {round(finish-start,2)} seconds')

def metrics_calculator(i):
    """
    Takes the range shape[0] to loop through
    """
    event=two_rain.iloc[row_start[i], col_start[i]:col_stop[i]]
    volume.append(event.sum())
    duration.append(event.shape[0])
    tracking_info.append(tracking.iloc[row_start[i], :])    



def metrics_calculator2(i):
    """
    Takes the range shape[0] to loop through
    """
    event=two_rain.iloc[row_start[i], col_start[i]:col_stop[i]]
    tracking_info=tracking.iloc[row_start[i], :]
    
    out=tracking_info.append(pd.Series({"Volume":event.sum(), 'Duration':event.shape[0]}))
    metrics2.append(out)
    
    return 



duration=[]
volume=[]
tracking_info=[]
    
start=time.perf_counter()

#Using process pool as it is ~3x faster than threaded 
with cf.ThreadPoolExecutor() as executor:
    executor.map(metrics_calculator, range(row_start.shape[0]))#map calls a function for each list element that is passed
    
metrics=pd.DataFrame(tracking_info)
metrics["Duration"]=duration
metrics["Volume"]=volume
metrics["Intensity"]=metrics["Volume"]/metrics["Duration"]



finish=time.perf_counter()

print(f'Metrics 1 finished in {round(finish-start,2)} seconds')



metrics2=[]   
start=time.perf_counter()

#Using process pool as it is ~3x faster than threaded 
with cf.ThreadPoolExecutor() as executor:
    executor.map(metrics_calculator2, range(row_start.shape[0]))#map calls a function for each list element that is passed
    
metrics2=pd.DataFrame(metrics2)
metrics2["Intensity"]=metrics2["Volume"]/metrics2["Duration"]



finish=time.perf_counter()

print(f'Metrics 2 finished in {round(finish-start,2)} seconds')
