import ray.dataframe as pd
#import pandas as pd

print('############ 4: Test Ray Convert [Column] toDateTime #############')

ray_df = pd.read_csv("yellow_tripdata_2015-01.csv")

print('    Read_CSV finished. Result:')
print(ray_df.head(3))

ray_df = ray_df.set_index('tpep_pickup_datetime') 

print('    set_index finished. Result:')
print(ray_df.head(3))

ray_df.index = pd.to_datetime(ray_df.index)
print(ray_df.head(3))

print('    to_datetime(df.index) finished. Result above.')