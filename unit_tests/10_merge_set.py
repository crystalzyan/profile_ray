import ray.dataframe as pd
#import pandas as pd

print('############ 10: Test Ray Merge #############')

ray_df = pd.read_csv("yellow_tripdata_2015-01.csv", 
         parse_dates=['tpep_pickup_datetime', 'tpep_dropoff_datetime'])

print('    Read_CSV finished. Result:')
print(ray_df.head(3))

ray_payments = pd.DataFrame({'num':[1, 2, 3, 4, 5, 6], 
              'payment_method':['Credit Card', 'Cash', 'No Charge', 
                              'Dispute', 'Unknown', 'Voided trip']})
print('    new DataFrame finished. Result:')
print(ray_payments)                              

ray_df2 = ray_df.merge(ray_payments, left_on="payment_type", right_on="num")
print('    merge finished. Result:')
print(ray_df2.head(3))

ray2_groupby = ray_df2.groupby(ray_df2.payment_name)
print('    groupby on merge finished. Result:')
print(ray2_groupby.head(3))

result = ray2_groupby.tip_amount.mean()
print('    mean on groupby finished. Result:')
print(result)

ray_bool1 = ray_df2.tip_amount == 0
ray_bool2 = ray_df2.payment_name == 'Cash'
print('    booleans finished. Result:')
print(ray_bool1.head(3))

ray_bools = pd.concat([ray_bool1, ray_bool2], axis=1)
print('    concat of booleans finished. Result:')
print(ray_bools.head(3))

result = ray_bools.corr()
print('    correlation finished. Result:')
print(result)

print('    Set of merge tests finished.')