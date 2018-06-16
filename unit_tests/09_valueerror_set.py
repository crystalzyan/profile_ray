import ray.dataframe as pd
#import pandas as pd

print('############ 9: Test Ray For ValueError #############')

ray_df = pd.read_csv("yellow_1of3.csv")

print('    Read_CSV finished. Result:')
print(ray_df.head(3))

# ray_df = ray_df.set_index('tpep_pickup_datetime') 

# print('    set_index finished. Result:')
# print(ray_df.head(3))

# ray_df.index = pd.to_datetime(ray_df.index)
# print('    to_datetime(df.index) finished. Result:')
# print(ray_df.head(3))

ray_df['tpep_pickup_datetime'] = pd.to_datetime(ray_df['tpep_pickup_datetime']) 
print('    to_datetime(df[column]) finished. Result:')
print(ray_df.head(3))

ray_df2 = ray_df[(ray_df['tip_amount'] > 0) & (ray_df['fare_amount'] > 0)]
print('    Eliminated invalid rows finished. Result:')
print(ray_df2.head(3))

print('    Possible ValueError incoming.')
ray_df2["tip_fraction"] = ray_df2['tip_amount'] / ray_df2['fare_amount']
print('    new column finished. Result:')
print(ray_df2.head(3))

print('    Possible ValueError incoming.')
# Groupby.__getitem__ NotImplemented Error. Will not pass here.
#day_of_week = (ray_df2.groupby(ray_df2.tpep_pickup_datetime.dt.dayofweek)['tip_fraction'].mean()) 
day_of_week = (ray_df2.groupby(ray_df2.tpep_pickup_datetime.dt.dayofweek).tip_fraction.mean())
print('    day_of_week. Result:')
print(day_of_week)

print('    Possible ValueError incoming.')
#hour = (ray_df2.groupby(ray_df2.tpep_pickup_datetime.dt.hour)['tip_fraction'].mean())
hour = (ray_df2.groupby(ray_df2.tpep_pickup_datetime.dt.hour).tip_fraction.mean())
print('    hour finished. Result:')
print(hour)

print('    Set of ValueError tests finished.')