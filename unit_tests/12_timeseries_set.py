import ray.dataframe as pd
#import pandas as pd

print('############ 12: Test Ray TimeSeries #############')

ray_df = pd.read_csv("yellow_tripdata_2015-01.csv")

print('    Read_CSV finished. Result:')
print(ray_df.head(3))

ray_df = ray_df.set_index('tpep_pickup_datetime') 

print('    set_index finished. Result:')
print(ray_df.head(3))

ray_df.index = pd.to_datetime(ray_df.index)
print('    to_datetime finished. Result:')
print(ray_df.head(3))

ray_resamp = ray_df.passenger_count.resample('1d')
print('    resample finished')

ray_mn = ray_resamp.mean()
print('    mean of resample finished. Result:')
print(ray_mn)

ray_roll = ray_df.passenger_count.rolling(10)
print('    rolling aggregation finished. Result:')
print(ray_roll)

result = ray_roll.mean()
print('    mean of rolling aggregation finished. Result:')
print(result)

print('    Set of shuffled index tests finished.')