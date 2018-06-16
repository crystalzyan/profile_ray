import ray.dataframe as pd
#import pandas as pd

print('############ 5: Test Ray .Column Filter #############')

ray_df = pd.read_csv("yellow_tripdata_2015-01-01.csv")

print('    Read_CSV finished. Result:')
print(ray_df.head(3))

ray_df2 = ray_df[(ray_df.tip_amount > 0) & (ray_df.fare_amount > 0)] 
print(ray_df2.head(3))

print('    filter df.column finished. Result above.')