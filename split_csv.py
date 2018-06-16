import pandas as pd

df = pd.read_csv("yellow_tripdata_2015-01.csv", 
     parse_dates=['tpep_pickup_datetime', 'tpep_dropoff_datetime'])

print('Finished reading in csv.')

# Split two-ways
# df_first = df[df['tpep_pickup_datetime'] <= '01/15/2015']
# df_second = df[df['tpep_pickup_datetime'] > '01/15/2015']

# print('length of first half ', len(df_first))
# print('length of second half ', len(df_second))

# df_first.to_csv("yellow_tripdata_2015-01-01.csv")
# df_second.to_csv("yellow_tripdata_2015-01-15.csv")

# Split three-ways
df_first = df[df['tpep_pickup_datetime'] <= '01/10/2015']
df_second = df[(df['tpep_pickup_datetime'] > '01/10/2015') & (df['tpep_pickup_datetime'] <= '01/20/2015')]
df_third = df[df['tpep_pickup_datetime'] > '01/20/2015']

print('length of first half ', len(df_first))
print('length of second half ', len(df_second))
print('length of third half ', len(df_third))

df_first.to_csv("yellow_1of3.csv")
df_second.to_csv("yellow_2of3.csv")
df_third.to_csv("yellow_3of3.csv")
