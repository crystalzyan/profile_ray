import ray.dataframe as pd
#import pandas as pd

print('############ 11: Test Ray SetIndex #############')

ray_df = pd.read_csv("yellow_tripdata_2015-01-01.csv")

print('    Read_CSV finished. Result:')
print(ray_df.head(3))

ray_df = ray_df.set_index('tpep_pickup_datetime') 

print('    set_index finished. Result:')
print(ray_df.head(3))

ray_df.index = pd.to_datetime(ray_df.index)
print('    to_datetime finished. Result:')
print(ray_df.head(3))

result = ray_df.tail()
print('    tail finished. Result:')
print(result)

# Ray fails at non-unique indices
# ray_loc = ray_df.loc['2015-01-10 19:01:44']
# print('    loc finished. Result:')
# print(ray_loc.head(3))



print('    Set of shuffled index tests finished. Result above.')