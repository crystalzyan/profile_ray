import ray.dataframe as pd
#import pandas as pd

print('############ 3: Test Ray Convert [Column] toDateTime #############')

ray_df = pd.read_csv("yellow_tripdata_2015-01.csv")

print('    Read_CSV finished. Result:')
print(ray_df.head(3))

ray_df['tpep_pickup_datetime'] = pd.to_datetime(ray_df['tpep_pickup_datetime']) 
print(ray_df.head(3))

print('    to_datetime(df[column]) finished. Result above.')