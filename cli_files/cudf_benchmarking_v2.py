import cudf
import dask_cudf
import pandas as pd 
import numpy as np
import re 
import argparse

from load_data_v2 import validate_args
from load_data_v2 import load_data_cudf
from load_data_v2 import load_data_df

from generate_graphs_v2 import generate_cudf_vs_pandas_p1
from generate_graphs_v2 import generate_cudf_vs_pandas_p2
from generate_graphs_v2 import generate_speedups 

import seaborn as sns
import matplotlib.pyplot as plt

#Timer Class for Benchmarking - Source: https://realpython.com/python-timer/

import time

class TimerError(Exception):
    """A custom exception used to report errors in use of Timer class"""

class Timer:
    total_time = 0
    def __init__(self):
        self._start_time = None

    def start(self):
        """Start a new timer"""
        if self._start_time is not None:
            raise TimerError(f"Timer is running. Use .stop() to stop it")

        self._start_time = time.perf_counter()

    def stop(self):
        """Stop the timer, and report the elapsed time"""
        if self._start_time is None:
            raise TimerError(f"Timer is not running. Use .start() to start it")

        elapsed_time = time.perf_counter() - self._start_time
        Timer.total_time = elapsed_time
        self._start_time = None
        print(f"Elapsed time: {elapsed_time:0.4f} seconds")
        

#Data Used for barplot for plotting times of performance of each operation for cudf and pandas (when -operation all is called)
        
graph_data = []

#Data Used to make barplot of operation vs. Speedup (cuDF)

cudf_barplot_data = []
        
def load_data():
        
    print("Pandas: Loading Taxi Data:")        
    load_df_timer = Timer()
    load_df_timer.start()
    pd.read_csv('yellow_tripdata_2019-12.csv')
    load_df_timer.stop()

    df_time = load_df_timer.total_time
    
    print("CuDF: Loading Taxi Data:")
    load_cudf_timer = Timer()
    load_cudf_timer.start()
    cudf.read_csv('yellow_tripdata_2019-12.csv')
    load_cudf_timer.stop()
    
    cudf_time = load_cudf_timer.total_time
    
    speedup = round(float(df_time) / float(cudf_time), 3)
    print("Speedup: " + str(speedup) + "X")

#Applying numerical function(s) to each column -> Converting trip distance (mi) to trip distance (km)

def apply_numerical_function(nyc_taxi_data, nyc_cudf_taxi_data):
    print("Pandas: Applying a numerical function (converting mi to km):")
    func_copy_nyc_taxi_data = nyc_taxi_data
    load_df_timer_func = Timer()
    load_df_timer_func.start()
    func_copy_nyc_taxi_data['trip_distance'] = func_copy_nyc_taxi_data['trip_distance'].apply(lambda x: 1.60934 * x)
    load_df_timer_func.stop()

    df_time = load_df_timer_func.total_time
    graph_data.append(["af", df_time, "pandas"])

    print("CuDF: Applying a numerical function (converting mi to km):")
    func_copy_nyc_taxi_data_cudf = nyc_cudf_taxi_data
    load_cudf_timer_func = Timer()
    load_cudf_timer_func.start()
    func_copy_nyc_taxi_data_cudf['trip_distance'] = func_copy_nyc_taxi_data_cudf['trip_distance'].applymap(lambda x: 1.60934 * x)
    load_cudf_timer_func.stop()

    cudf_time = load_cudf_timer_func.total_time
    graph_data.append(["af", cudf_time, "cuDF"])

    speedup = round(float(df_time) / float(cudf_time), 3)
    cudf_barplot_data.append(["af", speedup])
    print("Speedup: " + str(speedup) + "X")
        
        
# Applying aggregate function (.sum() in this case) on a certain column
        
def apply_aggregate_function(nyc_taxi_data, nyc_cudf_taxi_data):
        
    print("Pandas: Applying an aggregate function:")
    func_copy_nyc_taxi_data = nyc_taxi_data
    load_df_timer_afunc = Timer()
    load_df_timer_afunc.start()
    func_copy_nyc_taxi_data['fare_amount'].sum()
    load_df_timer_afunc.stop()

    df_time = load_df_timer_afunc.total_time
    graph_data.append(["aggfunc", df_time, "pandas"])

    print("CuDF: Applying an aggregate function:")
    func_copy_nyc_taxi_data_cudf = nyc_cudf_taxi_data
    load_cudf_timer_afunc = Timer()
    load_cudf_timer_afunc.start()
    func_copy_nyc_taxi_data_cudf['fare_amount'].sum()
    load_cudf_timer_afunc.stop()

    cudf_time = load_cudf_timer_afunc.total_time
    graph_data.append(["aggfunc", cudf_time, "cuDF"])

    speedup = round(float(df_time) / float(cudf_time), 3)
    cudf_barplot_data.append(["aggfunc", speedup])
    print("Speedup (cuDF): " + str(speedup) + "X")

        
#Applying groupby capabilities on cudf vs. pandas 

def groupby(nyc_taxi_data, nyc_cudf_taxi_data):

    print("Pandas: GroupBy:")
    df_groupby_timer = Timer()
    df_groupby_timer.start()
    nyc_taxi_data.groupby(['payment_type']).size()
    df_groupby_timer.stop()

    df_time = df_groupby_timer.total_time
    graph_data.append(["gb", df_time, "pandas"])

    print("CuDF: GroupBy: ")
    cudf_groupby_timer = Timer()
    cudf_groupby_timer.start()
    nyc_cudf_taxi_data.groupby(['payment_type']).size()
    cudf_groupby_timer.stop()

    cudf_time = cudf_groupby_timer.total_time
    graph_data.append(["gb", cudf_time, "cuDF"])

    speedup = round(float(df_time) / float(cudf_time), 3)
    cudf_barplot_data.append(["gb", speedup])
    print("Speedup: " + str(speedup) + "X")

#Checking concatenation speeds (for pandas df) --> NYC Taxi Data 


def concatenate_dataframes(nyc_taxi_data, nyc_cudf_taxi_data):
    
    
    print("Concatenating Dataframes (Vertically, Stacking) - Pandas")
    concat_df_timer = Timer()
    concat_df_timer.start()
    pd.concat(nyc_taxi_data)
    concat_df_timer.stop()
    
    df_time = concat_df_timer.total_time 
    
    print("Concatenating Dataframes (Vertically, Stacking) - CuDF")
    concat_cudf_timer = Timer()
    concat_cudf_timer.start()
    cudf.concat(nyc_cudf_taxi_data)
    concat_cudf_timer.stop()
    
    cudf_time = concat_cudf_timer.total_time
    
    speedup = round(float(df_time) / float(cudf_time), 5)
    print("Speedup (cuDF): " + str(speedup) + "X")
    
    '''
    
    print("Concatenating Dataframes (Vertically, Stacking) - Dask_cuDF")
    
    concat_dask_cudf_timer = Timer()
    concat_dask_cudf_timer.start()
    dask_cudf.concat(nyc_dask_cudf_taxi_data)
    concat_dask_cudf_timer.stop()
    
    dask_cudf_time = concat_dask_cudf_timer.total_time
    
    speedup = round(float(df_time) / float(dask_cudf_time), 5)
    print("Speedup (Dask cuDF): " + str(speedup) + "X")
    '''
    

#Sort values to get top K entries by column (e.x. top 5 highest values, top 10 lowest values, etc...)

def get_top_5(nyc_taxi_data, nyc_cudf_taxi_data):

    print("Get Top 5 Longest Trip Distances (Sorting values) w/ Pandas")

    df_topk_timer = Timer()
    df_topk_timer.start()
    t5_longest_taxi_distances = nyc_taxi_data.sort_values(by='trip_distance', ascending=False)
    df_topk_timer.stop()

    df_time = df_topk_timer.total_time
    graph_data.append(["gt5", df_time, "pandas"])

    print("Get Top 5 Longest Trip Distances (Sorting Values) w/ cuDF")

    cudf_top_k_timer = Timer()
    cudf_top_k_timer.start()
    t5_longest_taxi_distances_cudf = nyc_cudf_taxi_data.sort_values(by='trip_distance', ascending=False)
    cudf_top_k_timer.stop()

    cudf_time = cudf_top_k_timer.total_time
    graph_data.append(["gt5", cudf_time, "cuDF"])

    speedup = round(float(df_time) / float(cudf_time), 3)
    cudf_barplot_data.append(["gt5", speedup])
    print("Speedup: " + str(speedup) + "X")
        

    
def drop_duplicates(nyc_taxi_data, nyc_cudf_taxi_data):

    print("Dropping Duplicates - Pandas")
    df_duplicatedrop = nyc_taxi_data

    dup_drop_df_timer = Timer()
    dup_drop_df_timer.start()
    df_duplicatedrop = df_duplicatedrop.drop_duplicates()
    dup_drop_df_timer.stop()
    
    df_time = dup_drop_df_timer.total_time
    graph_data.append(["dd", df_time, "pandas"])

    print("Dropping Duplicates - cuDF")

    cudf_duplicatedrop = nyc_cudf_taxi_data

    dup_drop_cudf_timer = Timer()
    dup_drop_cudf_timer.start()
    cudf_duplicatedrop = cudf_duplicatedrop.drop_duplicates()
    dup_drop_cudf_timer.stop()
    
    cudf_time = dup_drop_cudf_timer.total_time
    graph_data.append(["dd", cudf_time, "cuDF"])
    
    speedup = round(float(df_time) / float(cudf_time), 3)
    cudf_barplot_data.append(["dd", speedup])
    print("Speedup: " + str(speedup) + "X")

#Dropping a Column - Pandas (on NYC Taxi Data)

def drop_column(nyc_taxi_data, nyc_cudf_taxi_data):
    

    print("Drop a column: Pandas")
    col_drop_df = nyc_taxi_data


    col_drop_df_timer = Timer()
    col_drop_df_timer.start()
    col_drop_df = col_drop_df.drop(['mta_tax'], axis=1)
    col_drop_df_timer.stop()

    df_time = col_drop_df_timer.total_time
    graph_data.append(["dc", df_time, "pandas"])

    #Dropping a Column - cuDF 

    print("Drop a column: cuDF")
    col_drop_cudf = nyc_cudf_taxi_data

    col_drop_cudf_timer = Timer()
    col_drop_cudf_timer.start()
    col_drop_cudf = col_drop_cudf.drop(['mta_tax'], axis=1)
    col_drop_cudf_timer.stop()

    cudf_time = col_drop_cudf_timer.total_time
    graph_data.append(["dc", cudf_time, "cuDF"])

    speedup = round(float(df_time) / float(cudf_time), 3)
    cudf_barplot_data.append(["dc", speedup])
    print("Speedup: " + str(speedup) + "X")
            

#Filtering Rows Based on their individual feature values - Pandas (on NYC Taxi Data)

#Example = Filter all trips where the total price of the taxi ride is greater than or equal to 20 dollars

def filter_by_column(nyc_taxi_data, nyc_cudf_taxi_data):
    

    print("Filter by Column Values - pandas")
    row_filt_df = nyc_taxi_data 

    row_filt_df_timer = Timer()
    row_filt_df_timer.start()
    row_filt_df = row_filt_df[row_filt_df['total_amount'] >= 20]
    row_filt_df_timer.stop()

    df_time = row_filt_df_timer.total_time
        
    graph_data.append(["fc", df_time, "pandas"]) #add for visualization

        #Filtering Rows Based on their individual feature values - cuDF (on NYC Taxi Data)

    print("Filter by Column Values - cuDF")
    row_filt_cudf = nyc_cudf_taxi_data

    row_filt_cudf_timer = Timer()
    row_filt_cudf_timer.start()
    row_filt_cudf = row_filt_cudf[row_filt_cudf['total_amount'] >= 20]
    row_filt_cudf_timer.stop()

    cudf_time = row_filt_cudf_timer.total_time
    graph_data.append(["fc", cudf_time, "cuDF"])

    speedup = round(float(df_time) / float(cudf_time), 3)
    cudf_barplot_data.append(["fc", speedup])
    print("Speedup (cuDF): " + str(speedup) + "X")

#Rewriting to .csv - Pandas

def rewrite_to_csv(nyc_taxi_data, nyc_cudf_taxi_data):

    print("Rewriting to .csv - Pandas")

    rewrite_df = nyc_taxi_data
    rewrite_df_timer = Timer()
    rewrite_df_timer.start()
    rewrite_df.to_csv("rewritten_nyc_taxi_data_df.csv")
    rewrite_df_timer.stop()
    
    df_time = rewrite_df_timer.total_time
    graph_data.append(["rtc", round(df_time, 3), "pandas"])

    print("Rewriting to .csv - cuDF")

    #Rewriting to .csv - cuDF 
    rewrite_cudf = nyc_cudf_taxi_data
    rewrite_cudf_timer = Timer()
    rewrite_cudf_timer.start()
    rewrite_cudf.to_csv("rewritten_nyc_taxi_data_cudf.csv")
    rewrite_cudf_timer.stop()
    
    cudf_time = rewrite_cudf_timer.total_time
    graph_data.append(["rtc", round(cudf_time, 3), "cuDF"])
    
    speedup = round(float(df_time) / float(cudf_time), 3)
    cudf_barplot_data.append(["rtc", speedup])
    print("Speedup: " + str(speedup) + "X")
    
def fill_null_values(nyc_taxi_data, nyc_cudf_taxi_data):
    
    print("Filling in null values - Pandas")
    
    copy_nyc_taxi_data = nyc_taxi_data
    fill_null_vals = Timer()
    fill_null_vals.start()
    copy_nyc_taxi_data = copy_nyc_taxi_data.fillna(0)
    fill_null_vals.stop()
    
    df_time = fill_null_vals.total_time
    graph_data.append(["fnv", round(df_time, 3), "pandas"])
    
    print("Filling in null values - cuDF")
    
    copy_nyc_cudf_taxi_data = nyc_cudf_taxi_data
    copy_nyc_cudf_taxi_data = copy_nyc_cudf_taxi_data.drop(columns=['store_and_fwd_flag'])
    fill_null_vals_cudf = Timer()
    fill_null_vals_cudf.start()
    copy_nyc_cudf_taxi_data = copy_nyc_cudf_taxi_data.fillna(0)
    fill_null_vals_cudf.stop()
    
    cudf_time = fill_null_vals_cudf.total_time
    graph_data.append(["fnv", round(cudf_time, 3), "cuDF"])
    
    speedup = round(float(df_time) / float(cudf_time), 3)
    cudf_barplot_data.append(["fnv", speedup])
    print("Speedup (cuDF): " + str(speedup) + "X")
    
def merge_data(df_data, cudf_data):
    
    rcid_to_meaning_df = pd.read_csv('rcid_to_meaning.csv')
    rcid_to_meaning_cudf = cudf.read_csv('rcid_to_meaning.csv')
        
    rcid_to_meaning_df.columns = ['rate_code', 'Meaning']
    rcid_to_meaning_cudf.columns = ['rate_code', 'Meaning']
    
    #rcid_to_meaning_df = rcid_to_meaning_df[]
        
    print("Inner Join (merge) - Pandas")
    df_merge_timer = Timer() 
    df_merge_timer.start()
    new_data_df = df_data.merge(rcid_to_meaning_df, on=['rate_code'], how='inner')
    df_merge_timer.stop()
        
    df_time = df_merge_timer.total_time 
    graph_data.append(["merge", round(df_time, 3), "pandas"])
        
    print("Inner Join (merge) - cuDF ")
    cudf_merge_timer = Timer()
    cudf_merge_timer.start()
    new_data_cudf = cudf_data.merge(rcid_to_meaning_cudf, on=['rate_code'], how='inner')
    cudf_merge_timer.stop()
        
    cudf_time = cudf_merge_timer.total_time 
    graph_data.append(["merge", round(cudf_time, 3), "cuDF"])
        
    speedup = round(float(df_time) / float(cudf_time), 3)
    cudf_barplot_data.append(["merge", speedup])
    print("Speedup (cuDF): " + str(speedup) + "X")
        
        
def main():
    
    #Take in arguments for pandas/cudf operation and desired size(?) of dataset 
    
    parser = argparse.ArgumentParser(description="Benchmark pandas vs. cuDF performance for various DataFrame operations")
    
    parser.add_argument("-num_months", default="1", type=str, help="Number of months of taxi data you want to load")
    
    parser.add_argument("-operation", default="None", type=str, help="Benchmarks pandas vs. cuDF performance for the operation specified")
    
    #Parse arguments and validate that datasets match the arguments, load the datasets 
    
    args = parser.parse_args()
    
    argmap = {}

    argmap['operation'] = args.operation
    argmap['num_months'] = args.num_months 

    
    valid_args = validate_args(argmap)
    
    
    if valid_args == True:
        
        print("Validated Arguments Successfully")
        
        #Adding timer for loading data -cudf
        print("Loading data through cudf ...")
        
        load_cudf_data_timer = Timer()
        load_cudf_data_timer.start()
        cudf_data = load_data_cudf(argmap)
        load_cudf_data_timer.stop()
        
        cudf_time = load_cudf_data_timer.total_time 
        #graph_data.append(["ld", round(cudf_time, 3), "cuDF"])
        
        #Adding timer for loading data - pandas 
        print("Loading data through pandas ...")
        
        load_df_data_timer = Timer()
        load_df_data_timer.start()
        df_data = load_data_df(argmap)
        load_df_data_timer.stop()
        
        df_time = load_df_data_timer.total_time 
        
        graph_data.append(["ld", round(df_time, 3), "pandas"])
        graph_data.append(["ld", round(cudf_time, 3), "cuDF"])
    
        speedup = round(float(df_time) / float(cudf_time), 3)
        cudf_barplot_data.append(["ld", speedup])
        
        print("Data Loading Speedup (cuDF): " + str(speedup) + "X")
        
        if args.operation == 'af':
            apply_numerical_function(df_data, cudf_data)
        if args.operation == 'aggfunc':
            apply_aggregate_function(df_data, cudf_data)
        if args.operation == 'gb':
            groupby(df_data, cudf_data)
        if args.operation == 'gt5': #uses sort_values()
            get_top_5(df_data, cudf_data)
        if args.operation == 'dd':
            drop_duplicates(df_data, cudf_data)
        if args.operation == 'dc':
            drop_column(df_data, cudf_data)
        if args.operation == 'fc':
            filter_by_column(df_data, cudf_data)
        if args.operation == 'rtc':
            rewrite_to_csv(df_data, cudf_data)
        if args.operation == 'cdf':
            concatenate_dataframes(df_data, cudf_data)
        if args.operation == 'fnv':
            fill_null_values(df_data, cudf_data)
        if args.operation == 'merge':
            merge_data(df_data, cudf_data)
        if args.operation == 'all':
            apply_numerical_function(df_data, cudf_data)
            apply_aggregate_function(df_data, cudf_data)
            groupby(df_data, cudf_data)
            get_top_5(df_data, cudf_data)
            drop_duplicates(df_data, cudf_data)
            drop_column(df_data, cudf_data)
            filter_by_column(df_data, cudf_data)
            rewrite_to_csv(df_data, cudf_data)
            fill_null_values(df_data, cudf_data)
            merge_data(df_data, cudf_data)
            #concatenate_dataframes(df_data, cudf_data)
            #print(graph_data)
        
            
            perf_data = pd.DataFrame(np.array(graph_data), columns=['operation', 'time (in s)', 'library'])
            perf_data['time (in s)'] = perf_data['time (in s)'].astype(float)
            
            
            
            # cuDF vs. pandas times -> Part I (less intensive operations)
            
            generate_cudf_vs_pandas_p1(perf_data)
            
            # cuDF vs. pandas times --> Part II (more intensive operations)
            
            generate_cudf_vs_pandas_p2(perf_data)
            
            # Speedups 
            
            generate_speedups(cudf_barplot_data)
            
            # "ld", "rtc", "aggfunc" -> more intensive
            
            '''
            
            
            copy_perf_data_p1 = perf_data 
            perf_data_p1 = copy_perf_data_p1[copy_perf_data_p1['operation'].isin(["af", "aggfunc", "gb", "dc", "fc", "fnv", "merge"])]
            library = perf_data_p1['library']
            colors = ['green' if l == 'cuDF' else 'grey' for l in library]
            sns_plot_p1 = sns.barplot(x='operation', y='time (in s)', hue='library', data=perf_data_p1, palette=colors, ci=None)
            plt.title("Pandas/cuDF operation vs. Time - Part I ")
            sns_plot_p1.figure.savefig("cudf_vs_pandas_p1.png")
            
            # cuDF vs. pandas times -> Part II (more intensive operations)
            
            copy_perf_data_p2 = perf_data 
            perf_data_p2 = copy_perf_data_p2[copy_perf_data_p2['operation'].isin(["ld", "rtc", "dd", "gt5"])]
            library = perf_data_p2['library']
            colors = ['green' if l == 'cuDF' else 'grey' for l in library]
            sns_plot_p2 = sns.barplot(x='operation', y='time (in s)', hue='library', data=perf_data_p2, palette=colors, ci=None)
            plt.title("Pandas/cuDF operation vs. Time - Part II ")
            sns_plot_p2.figure.savefig("cudf_vs_pandas_p2.png")
            
            
            
            # Speedups
            
            
            speedups_df = pd.DataFrame(np.array(cudf_barplot_data), columns=['operation', 'speedups (X times faster than pandas)'])
            speedups_df['speedups (X times faster than pandas)'] = speedups_df['speedups (X times faster than pandas)'].astype(float)
            colors = ['green' for i in range(len(speedups_df))]
            sns_plot_speedups = sns.barplot(x='operation', y='speedups (X times faster than pandas)', data=speedups_df, palette=colors, ci=None)
            plt.title("cuDF Speedup vs. Pandas")
            sns_plot_speedups.figure.savefig("speedups.png")
            '''
            
    else:
        print(valid_args)

if __name__ == "__main__":
    main()