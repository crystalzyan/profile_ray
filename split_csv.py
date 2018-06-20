import pandas as pd

df = pd.read_csv("yellow_tripdata_2015-01.csv", 
     parse_dates=['tpep_pickup_datetime', 'tpep_dropoff_datetime'])

print('Finished reading in csv.')

split = int(input('Ways to split? (Enter 2, 3, 4, or 5):'))

if split == 2:
	#Split two-ways
	df_first = df[df['tpep_pickup_datetime'] <= '01/15/2015']
	df_second = df[df['tpep_pickup_datetime'] > '01/15/2015']

	print('length of first half ', len(df_first))
	print('length of second half ', len(df_second))

	df_first.to_csv("yellow_tripdata_2015-01-01.csv")
	df_second.to_csv("yellow_tripdata_2015-01-15.csv")

elif split == 3:
	#Split three-ways
	df_first = df[df['tpep_pickup_datetime'] <= '01/10/2015']
	df_second = df[(df['tpep_pickup_datetime'] > '01/10/2015') & (df['tpep_pickup_datetime'] <= '01/20/2015')]
	df_third = df[df['tpep_pickup_datetime'] > '01/20/2015']

	print('length of first half ', len(df_first))
	print('length of second half ', len(df_second))
	print('length of third half ', len(df_third))

	df_first.to_csv("yellow_1of3.csv")
	df_second.to_csv("yellow_2of3.csv")
	df_third.to_csv("yellow_3of3.csv")

elif split == 4:
	# Split four-ways
	df_first = df[df['tpep_pickup_datetime'] <= '01/08/2015']
	df_second = df[(df['tpep_pickup_datetime'] > '01/08/2015') & (df['tpep_pickup_datetime'] <= '01/16/2015')]
	df_third = df[(df['tpep_pickup_datetime'] > '01/16/2015') & (df['tpep_pickup_datetime'] <= '01/24/2015')]
	df_fourth = df[df['tpep_pickup_datetime'] > '01/24/2015']

	print('length of first half ', len(df_first))
	print('length of second half ', len(df_second))
	print('length of third half ', len(df_third))
	print('length of fourth half ', len(df_fourth))

	df_first.to_csv("yellow_1of4.csv")
	df_second.to_csv("yellow_2of4.csv")
	df_third.to_csv("yellow_3of4.csv")
	df_fourth.to_csv("yellow_4of4.csv")

elif split == 5:
	# Split five-ways
	df_first = df[df['tpep_pickup_datetime'] <= '01/06/2015']
	df_second = df[(df['tpep_pickup_datetime'] > '01/06/2015') & (df['tpep_pickup_datetime'] <= '01/12/2015')]
	df_third = df[(df['tpep_pickup_datetime'] > '01/12/2015') & (df['tpep_pickup_datetime'] <= '01/18/2015')]
	df_fourth = df[(df['tpep_pickup_datetime'] > '01/18/2015') & (df['tpep_pickup_datetime'] <= '01/24/2015')]
	df_fifth = df[df['tpep_pickup_datetime'] > '01/24/2015']

	print('length of first half ', len(df_first))
	print('length of second half ', len(df_second))
	print('length of third half ', len(df_third))
	print('length of fourth half ', len(df_fourth))
	print('length of fifth half ', len(df_fifth))

	df_first.to_csv("yellow_1of5.csv")
	df_second.to_csv("yellow_2of5.csv")
	df_third.to_csv("yellow_3of5.csv")
	df_fourth.to_csv("yellow_4of5.csv")
	df_fifth.to_csv("yellow_5of5.csv")

