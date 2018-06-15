import ray.dataframe as pd
#import pandas as pd

@profile
def basic_set(csvStr):
	# Import CSV
	ray_df = pd.read_csv(csvStr) 
	ray_df2 = pd.read_csv(csvStr, 
         parse_dates=['tpep_pickup_datetime', 'tpep_dropoff_datetime'])

	# Accesses
	result = ray_df.head()
	result2 = ray_df2.head()

	return ray_df


@profile
def aggregation_set(ray_df):
	# Convert and len
	ray_df.tpep_pickup_datetime = pd.to_datetime(ray_df.tpep_pickup_datetime) 
	result = len(ray_df)

	# Groupby
	ray_groupby = ray_df.groupby(ray_df.passenger_count)
	result = ray_groupby.trip_distance.mean()

	# Filter, new column
	ray_df2 = ray_df[(ray_df.tip_amount > 0) & (ray_df.fare_amount > 0)]
	ray_df2["tip_fraction"] = ray_df2.tip_amount / ray_df2.fare_amount

	# Groupby and mean on filtered
	day_of_week = (ray_df2.groupby(ray_df2.tpep_pickup_datetime.dt.dayofweek)['tip_fraction'].mean())
  	hour = (ray_df2.groupby(ray_df2.tpep_pickup_datetime.dt.hour)['tip_fraction'].mean())
  

@profile
def merge_set(ray_df):
	# Merge
	ray_payments = pd.DataFrame({'num':[1, 2, 3, 4, 5, 6], 
	              'payment_method':['Credit Card', 'Cash', 'No Charge', 
	                              'Dispute', 'Unknown', 'Voided trip']})
	ray_df2 = ray_df.merge(ray_payments, left_on="payment_type", right_on="num")

	# Groupby on Merge
	ray2_groupby = ray_df2.groupby(ray_df2.payment_name)
	result = ray2_groupby.tip_amount.mean()

	# Boolean correlation
	ray_bool1 = ray_df2.tip_amount == 0
	ray_bool2 = ray_df2.payment_name == 'Cash'
	ray_bools = pd.concat([ray_bool1, ray_bool2], axis=1)
	result = ray_bools.corr()


@profile
def shuffle_set(ray_df):
	# Shuffle and convert
	ray_df = ray_df.set_index('tpep_pickup_datetime') 
	ray_df.index = pd.to_datetime(ray_df.index)

	# Accesses
	result = ray_df.head()
	result = ray_df.tail()
	ray_loc = ray_df.loc['2015-01-10 19:01:44']
	result = ray_loc.head()


@profile
def timeseries_set(ray_df):
	# Shuffle and convert
	ray_df = ray_df.set_index('tpep_pickup_datetime') 
	ray_df.index = pd.to_datetime(ray_df.index)

	# Resample
	ray_resamp = ray_df.passenger_count.resample('1d')
	ray_mn = ray_resamp.mean()

	# Rolling aggregation
	ray_roll = ray_df.passenger_count.rolling(10)
	result = ray_roll.mean()


def main():
	csvStr = "yellow_tripdata_2015-01.csv"
	df = basic_set(csvStr)
	aggregation_set(df)
	merge_set(df)
	shuffle_set(df)
	timeseries_set(df)


if __name__ == "__main__":
	main()
