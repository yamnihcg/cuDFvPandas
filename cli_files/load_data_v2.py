import pandas as pd
import cudf
import dask_cudf

'''

The function below makes sure that the operation that the user wants to run benchmarks against is in the list of supported operations. The function
also makes sure that the user doesn't enter more months than the amount of months for which data exists.

'''

def validate_args(argmap):
    
    valid_operations = ['af', 'gb', 'gt5', 'dd', 'dc', 'fc', 'rtc', 'ld', 'all', 'cdf', 'fnv', 'aggfunc', 'merge']
    check_operation = argmap['operation'] in valid_operations
    
    num_months = argmap['num_months']
    
    if check_operation:
        if int(num_months) <= 36:
            return True
        else: 
            return "Invalid Arguments Inputted. Refer to documentation, examples, and the files in the GCP Bucket for additional info"
        
'''
Checks which years data needs to be pulled from:

Category 1 - 2014
Category 2 - 2014, 2015
Category 3 - 2014, 2015, 2016

'''
def get_category(num_months):
    
    #Category 1 --> 1-12 Months
    if num_months >= 1 and num_months <= 12:
        return 1
    
    #Category 2 --> 13-24 Months
    if num_months >= 13 and num_months <= 24:
        return 2
    
    #Category 3 --> 25-36 Months
    if num_months >= 25 and num_months <= 36:
        return 3
    
# Relabel column names (e.x. VendorID --> vendor_id) so that month-to-month data can be merged
    
def relabel_taxi_data(monthly_taxi_data):
    monthly_taxi_data.columns = ['vendor_id', 'pickup_datetime', 'dropoff_datetime',
       'passenger_count', 'trip_distance', 'pickup_longitude',
       'pickup_latitude', 'rate_code', 'store_and_fwd_flag',
       'dropoff_longitude', 'dropoff_latitude', 'payment_type',
       'fare_amount', 'surcharge', 'mta_tax', 'tip_amount',
       'tolls_amount', 'total_amount']
    return monthly_taxi_data
    
# Relabel column names (e.x. VendorID --> vendor_id) so that month-to-month data can be merged
def relabel_taxi_data_v2(monthly_taxi_data):
    monthly_taxi_data = monthly_taxi_data.drop(columns=['improvement_surcharge'])
    monthly_taxi_data.columns = ['vendor_id', 'pickup_datetime', 'dropoff_datetime',
       'passenger_count', 'trip_distance', 'pickup_longitude',
       'pickup_latitude', 'rate_code', 'store_and_fwd_flag',
       'dropoff_longitude', 'dropoff_latitude', 'payment_type',
       'fare_amount', 'surcharge', 'mta_tax', 'tip_amount',
       'tolls_amount', 'total_amount']
    return monthly_taxi_data

        
#Pulls month by month data based on the number of months needed through cudf

def load_data_cudf(argmap):
    
    num_months = int(argmap['num_months'])
    operation = argmap['operation']
    
    category = get_category(num_months)
    
    datasets = []
    

    if category == 1:
        for m in range(1, num_months + 1):
            #Necessary because filepath for Jan - Sept ends in '2014-01.csv' but the filepath for Oct - Dec ends in '2014-11.csv'
            if m >= 1 and m <= 9:
                monthly_taxi_data = cudf.read_csv('gcs://anaconda-public-data/nyc-taxi/csv/2014/yellow' + '_tripdata_' + '2014-0' + str(m) + '.csv')
                monthly_taxi_data = relabel_taxi_data(monthly_taxi_data)
                datasets.append(monthly_taxi_data)
            else:
                monthly_taxi_data = cudf.read_csv('gcs://anaconda-public-data/nyc-taxi/csv/2014/yellow' + '_tripdata_' + '2014-' + str(m) + '.csv')
                monthly_taxi_data = relabel_taxi_data(monthly_taxi_data)
                datasets.append(monthly_taxi_data)
                
        '''If the user wants to benchmark concatenating data in pandas/cudf, then this returns the month-by-month data
        Otherwise, the month-by-month data is combined together'''
        if operation == 'cdf':
            return datasets
        else:
            final_dataset = cudf.concat(datasets)
            return final_dataset
                
    if category == 2:
        
        rem_mo = num_months - 12
        
        for m in range(1, 13):
            if m >= 1 and m <= 9:
                monthly_taxi_data = cudf.read_csv('gcs://anaconda-public-data/nyc-taxi/csv/2014/yellow' + '_tripdata_' + '2014-0' + str(m) + '.csv')
                monthly_taxi_data = relabel_taxi_data(monthly_taxi_data)
                datasets.append(monthly_taxi_data)
            else:
                monthly_taxi_data = cudf.read_csv('gcs://anaconda-public-data/nyc-taxi/csv/2014/yellow' + '_tripdata_' + '2014-' + str(m) + '.csv')
                monthly_taxi_data = relabel_taxi_data(monthly_taxi_data)
                datasets.append(monthly_taxi_data)
                
        for m in range(1, rem_mo + 1):
            if m >= 1 and m <= 9:
                monthly_taxi_data = cudf.read_csv('gcs://anaconda-public-data/nyc-taxi/csv/2015/yellow' + '_tripdata_' + '2015-0' + str(m) + '.csv')
                monthly_taxi_data = relabel_taxi_data_v2(monthly_taxi_data)
                datasets.append(monthly_taxi_data)
            else:
                monthly_taxi_data = cudf.read_csv('gcs://anaconda-public-data/nyc-taxi/csv/2015/yellow' + '_tripdata_' + '2015-' + str(m) + '.csv')
                monthly_taxi_data = relabel_taxi_data_v2(monthly_taxi_data)
                datasets.append(monthly_taxi_data)
                
        if operation == 'cdf':
            return datasets
        else:
            final_dataset = cudf.concat(datasets)
            return final_dataset
                
    if category == 3:
        
        rem_mo = num_months - 24
        
        for m in range(1, 13):
            
            if m >= 1 and m <= 9:
                monthly_taxi_data = cudf.read_csv('gcs://anaconda-public-data/nyc-taxi/csv/2014/yellow' + '_tripdata_' + '2014-0' + str(m) + '.csv')
                monthly_taxi_data_2015 = cudf.read_csv('gcs://anaconda-public-data/nyc-taxi/csv/2015/yellow' + '_tripdata_' + '2015-0' + str(m) + '.csv')
                monthly_taxi_data = relabel_taxi_data(monthly_taxi_data)
                monthly_taxi_data_2015 = relabel_taxi_data_v2(monthly_taxi_data_2015)
                datasets.append(monthly_taxi_data)
                datasets.append(monthly_taxi_data_2015)
            else:
                monthly_taxi_data = cudf.read_csv('gcs://anaconda-public-data/nyc-taxi/csv/2014/yellow' + '_tripdata_' + '2014-' + str(m) + '.csv')
                monthly_taxi_data_2015 = cudf.read_csv('gcs://anaconda-public-data/nyc-taxi/csv/2015/yellow' + '_tripdata_' + '2015-' + str(m) + '.csv')
                monthly_taxi_data = relabel_taxi_data(monthly_taxi_data)
                monthly_taxi_data_2015 = relabel_taxi_data_v2(monthly_taxi_data_2015)
                datasets.append(monthly_taxi_data)
                datasets.append(monthly_taxi_data_2015)
                
        for m in range(1, rem_mo + 1):
            
            if m >= 1 and m <= 9:
                monthly_taxi_data = cudf.read_csv('gcs://anaconda-public-data/nyc-taxi/csv/2016/yellow' + '_tripdata_' + '2016-0' + str(m) + '.csv')
                monthly_taxi_data = relabel_taxi_data_v2(monthly_taxi_data_2015)
                datasets.append(monthly_taxi_data)
            else:
                monthly_taxi_data = cudf.read_csv('gcs://anaconda-public-data/nyc-taxi/csv/2016/yellow' + '_tripdata_' + '2016-' + str(m) + '.csv')
                monthly_taxi_data = relabel_taxi_data_v2(monthly_taxi_data_2015)
                datasets.append(monthly_taxi_data)
                
        if operation == 'cdf':
            return datasets
        else:
            final_dataset = cudf.concat(datasets)
            return final_dataset
        

        
def load_data_df(argmap):
    
    num_months = int(argmap['num_months'])
    operation = argmap['operation']
    
    category = get_category(num_months)
    
    datasets = []
    

    if category == 1:
        for m in range(1, num_months + 1):
            if m >= 1 and m <= 9:
                monthly_taxi_data = pd.read_csv('gcs://anaconda-public-data/nyc-taxi/csv/2014/yellow' + '_tripdata_' + '2014-0' + str(m) + '.csv')
                monthly_taxi_data = relabel_taxi_data(monthly_taxi_data)
                datasets.append(monthly_taxi_data)
            else:
                monthly_taxi_data = pd.read_csv('gcs://anaconda-public-data/nyc-taxi/csv/2014/yellow' + '_tripdata_' + '2014-' + str(m) + '.csv')
                monthly_taxi_data = relabel_taxi_data(monthly_taxi_data)
                datasets.append(monthly_taxi_data)
        
        if operation == 'cdf':
            return datasets
        else:
            final_dataset = pd.concat(datasets)
            return final_dataset
                
    if category == 2:
        
        rem_mo = num_months - 12
        
        for m in range(1, 13):
            if m >= 1 and m <= 9:
                monthly_taxi_data = pd.read_csv('gcs://anaconda-public-data/nyc-taxi/csv/2014/yellow' + '_tripdata_' + '2014-0' + str(m) + '.csv')
                monthly_taxi_data = relabel_taxi_data(monthly_taxi_data)
                datasets.append(monthly_taxi_data)
            else:
                monthly_taxi_data = pd.read_csv('gcs://anaconda-public-data/nyc-taxi/csv/2014/yellow' + '_tripdata_' + '2014-' + str(m) + '.csv')
                monthly_taxi_data = relabel_taxi_data(monthly_taxi_data)
                datasets.append(monthly_taxi_data)
                
        for m in range(1, rem_mo + 1):
            if m >= 1 and m <= 9:
                monthly_taxi_data = pd.read_csv('gcs://anaconda-public-data/nyc-taxi/csv/2015/yellow' + '_tripdata_' + '2015-0' + str(m) + '.csv')
                monthly_taxi_data = relabel_taxi_data_v2(monthly_taxi_data)
                datasets.append(monthly_taxi_data)
            else:
                monthly_taxi_data = pd.read_csv('gcs://anaconda-public-data/nyc-taxi/csv/2015/yellow' + '_tripdata_' + '2015-' + str(m) + '.csv')
                monthly_taxi_data = relabel_taxi_data_v2(monthly_taxi_data)
                datasets.append(monthly_taxi_data)
                
        if operation == 'cdf':
            return datasets
        else:
            final_dataset = pd.concat(datasets)
            return final_dataset
                
    if category == 3:
        
        rem_mo = num_months - 24
        
        for m in range(1, 13):
            
            if m >= 1 and m <= 9:
                monthly_taxi_data = pd.read_csv('gcs://anaconda-public-data/nyc-taxi/csv/2014/yellow' + '_tripdata_' + '2014-0' + str(m) + '.csv')
                monthly_taxi_data_2015 = pd.read_csv('gcs://anaconda-public-data/nyc-taxi/csv/2015/yellow' + '_tripdata_' + '2015-0' + str(m) + '.csv')
                monthly_taxi_data = relabel_taxi_data(monthly_taxi_data)
                monthly_taxi_data_2015 = relabel_taxi_data_v2(monthly_taxi_data_2015)
                datasets.append(monthly_taxi_data)
                datasets.append(monthly_taxi_data_2015)
            else:
                monthly_taxi_data = pd.read_csv('gcs://anaconda-public-data/nyc-taxi/csv/2014/yellow' + '_tripdata_' + '2014-' + str(m) + '.csv')
                monthly_taxi_data_2015 = pd.read_csv('gcs://anaconda-public-data/nyc-taxi/csv/2015/yellow' + '_tripdata_' + '2015-' + str(m) + '.csv')
                monthly_taxi_data = relabel_taxi_data(monthly_taxi_data)
                monthly_taxi_data_2015 = relabel_taxi_data_v2(monthly_taxi_data_2015)
                datasets.append(monthly_taxi_data)
                datasets.append(monthly_taxi_data_2015)
                
        for m in range(1, rem_mo + 1):
            
            if m >= 1 and m <= 9:
                monthly_taxi_data = pd.read_csv('gcs://anaconda-public-data/nyc-taxi/csv/2016/yellow' + '_tripdata_' + '2016-0' + str(m) + '.csv')
                monthly_taxi_data = relabel_taxi_data_v2(monthly_taxi_data_2015)
                datasets.append(monthly_taxi_data)
            else:
                monthly_taxi_data = pd.read_csv('gcs://anaconda-public-data/nyc-taxi/csv/2016/yellow' + '_tripdata_' + '2016-' + str(m) + '.csv')
                monthly_taxi_data = relabel_taxi_data_v2(monthly_taxi_data_2015)
                datasets.append(monthly_taxi_data)
                
        if operation == 'cdf':
            return datasets
        else:
            final_dataset = pd.concat(datasets)
            return final_dataset

