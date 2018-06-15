import ray.dataframe as pd
#import pandas as pd

print('############ 1: Test Ray First on Group By #############')

ray_df = pd.read_csv("yellow_tripdata_2015-01.csv", 
         parse_dates=['tpep_pickup_datetime', 'tpep_dropoff_datetime'])

print('    Read_CSV finished. Result:')
print(ray_df.head(3))

ray_groupby = ray_df.groupby(ray_df.passenger_count)

print('    Groupby finished. Result of groupby.first:')

print(ray_groupby.first())

print('    Groupby.first finished')