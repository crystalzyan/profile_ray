import ray.dataframe as pd
import pandas as old_pd
import datetime

print('Running profile on Ray using 1/12 of data from Dask example')
print('Data used in this example is 1.99 GB')


total_t = datetime.timedelta()        # Total time taken for current example step
ray_session_t = datetime.timedelta()  # Sums up total_t for steps done by Ray
pd_session_t = datetime.timedelta()   # Sums up total_t for steps done by Pandas

runToCompletion = True    # Set to True to skip Ray parts that may hang
runPandas = True           # Set to False to only run Ray, not Ray and Pandas

printResults = True       # Set as False to not output results at each step
topN = 3                  # Number of rows used with df.head() for printing results
topGroupN = 1             # Number of rows used with df_groupby.head() for printing results

# Print out Ray's or Pandas's result on data
def printRes(res, isRay):
  if printResults:
    print('')
    if isRay:
      print("Ray's result:")
    else:
      print("Pandas's result:")

    print(res)
    print('')

# Print out and save Ray's or Pandas's execution time
def printTimer(time_captured, isRay, skipThis=False):
  if skipThis:
    if isRay:
      print("!!! Skipping for Ray !!!")
    else:
      print("!!! Skipping for Pandas !!!")
      print('')
  else:
    if isRay:
      global ray_session_t
      ray_session_t = ray_session_t + time_captured
      print('Ray took (hh:mm:ss.ms) {}'.format(total_t))
    else:
      global pd_session_t
      pd_session_t = pd_session_t + time_captured
      print('Pandas took (hh:mm:ss.ms) {}'.format(total_t))
      print('')


############## CSV DATA AND BASIC OPERATIONS #################
print('############# SECTION: CSV DATA AND BASIC OPERATIONS #############')
print('')

# Read in csv data
print('01: Profiling df = read_csv()')
print('--------------------------------------------------')
start_t = datetime.datetime.now()
#ray_df = pd.read_csv("yellow_tripdata_2015-01.csv") 
ray_df = pd.read_csv("yellow_tripdata_2015-01.csv", 
         parse_dates=['tpep_pickup_datetime', 'tpep_dropoff_datetime'])
total_t = datetime.datetime.now() - start_t
printTimer(total_t, True)

pd_df = None
if runPandas:
  start_t = datetime.datetime.now()
  #pd_df = old_pd.read_csv("yellow_tripdata_2015-01.csv")
  pd_df = old_pd.read_csv("yellow_tripdata_2015-01.csv", 
          parse_dates=['tpep_pickup_datetime', 'tpep_dropoff_datetime'])
  total_t = datetime.datetime.now() - start_t
  printTimer(total_t, False)


# View top rows of dataframe
print('02: Profiling df.head()')
print('--------------------------------------------------')
start_t = datetime.datetime.now()
result = ray_df.head()
total_t = datetime.datetime.now() - start_t
printRes(result, True)
printTimer(total_t, True)

if runPandas:
  start_t = datetime.datetime.now()
  result = pd_df.head()
  total_t = datetime.datetime.now() - start_t
  printRes(result, False)
  printTimer(total_t, False)



############## BASIC AGGREGATIONS AND GROUPBYS #################
print('############# SECTION: BASIC AGGREGATIONS AND GROUPBYS #############')
print('')

# Calculate length of dataframe
print('03: Profiling len(df)')
print('--------------------------------------------------')
start_t = datetime.datetime.now()
result = len(ray_df)
total_t = datetime.datetime.now() - start_t
printRes(result, True)
printTimer(total_t, True)

if runPandas:
  start_t = datetime.datetime.now()
  result = len(pd_df)
  total_t = datetime.datetime.now() - start_t
  printRes(result, False)
  printTimer(total_t, False)


# Groupby column
print('04: Profiling df_groupby = df.groupby(df.column)')
print('--------------------------------------------------')
start_t = datetime.datetime.now()
ray_groupby = ray_df.groupby(ray_df.passenger_count)
total_t = datetime.datetime.now() - start_t
result = ray_groupby.head(topGroupN)
printRes(result, True)
printTimer(total_t, True)

pd_groupby = None
if runPandas:
  start_t = datetime.datetime.now()
  pd_groupby = pd_df.groupby(pd_df.passenger_count)
  total_t = datetime.datetime.now() - start_t
  result = pd_groupby.head(topGroupN)
  printRes(result, False)
  printTimer(total_t, False)


# Calculate mean on groupby
print('05: Profiling df_groupby.column.mean()')
print('--------------------------------------------------')
print("!!! Ray does not support getting columns from groupbys !!!")
print("!!!     df.groupby.COLUMN_NAME: Ray raises AttributeError - can't find attribute COLUMN_NAME !!!")
print("!!!     df.groupby['COLUMN_NAME']: Ray raises NotImplementedError !!!")
if runToCompletion or True:  # Always skip
  printTimer(total_t, True, True)
else:
  start_t = datetime.datetime.now()

  # result = ray_groupby.trip_distance.mean()   # Raises AttributeError

  # Alternative code
  result = ray_groupby['trip_distance'].mean()  # Raises NotImplementedError

  total_t = datetime.datetime.now() - start_t
  printRes(result, True)
  printTimer(total_t, True)

if runPandas:
  start_t = datetime.datetime.now()
  result = pd_groupby.trip_distance.mean()
  total_t = datetime.datetime.now() - start_t
  printRes(result, False)
  printTimer(total_t, False)


# Added to convert date column to proper DateTime type for later parts
print('06: Profiling df.COLUMN_NAME = to_datetime(df.COLUMN_NAME)')
print('--------------------------------------------------')
print("!!! In some runs, Ray may spam complaints and hangs at this line !!!")
print("!!!     to_datetime(df['COLUMN_NAME']): Ray gives error 'task is a driver task and objects created by ray.put could not be reconstructed'. !!!")
print("!!!         df['COLUMN_NAME']: Ray then spams 'suppressing duplicate error message' until interrupted !!!")
print("!!!         df['COLUMN_NAME']: Ray may also spam 'ObjectID already exists in the object store' !!!")
print("!!!     to_datetime(df.COLUMN_NAME): Ray takes 17 seconds with this !!!")
print("!!!     to_datetime(df.index): Ray may yield ValueErrors in later parts !!!")
# If skip this, may also need to skip day_of_week/hour calculation step 09 later. Remember TimeSeries section requires DateTime conversion
if runToCompletion or True: # Always skip this   
  print("!!!     Skipping this for now to guarantee run to completion of this script !!!")
  printTimer(total_t, True, True)
else:
  start_t = datetime.datetime.now()
  
  # Always hangs?
  # ray_df['tpep_pickup_datetime'] = pd.to_datetime(ray_df['tpep_pickup_datetime'])  
  
  # Alternative code 1 - Dot syntax for column seems to work here. But takes 17 seconds!
  ray_df.tpep_pickup_datetime = pd.to_datetime(ray_df.tpep_pickup_datetime)  #

  # Alternative code 2 - Changes index. May yield ValueErrors in later parts
  # ray_df = ray_df.set_index('tpep_pickup_datetime') 
  # ray_df.index = pd.to_datetime(ray_df.index)
  
  total_t = datetime.datetime.now() - start_t
  result = ray_df.head(topN)
  printRes(result, True)
  printTimer(total_t, True)

if runPandas:
  start_t = datetime.datetime.now()
  pd_df['tpep_pickup_datetime'] = old_pd.to_datetime(pd_df['tpep_pickup_datetime'])
  total_t = datetime.datetime.now() - start_t
  result = pd_df.head(topN)
  printRes(result, False)
  printTimer(total_t, False)


# Filter valid rows
print('07: Profiling df2 = df[df.column > 0 & df.column > 0]')
print('--------------------------------------------------')
print("!!! In some runs, Ray may spam complaints and/or hang at this line. !!!")
print("!!!         Ray gives error 'task is a driver task and objects created by ray.put could not be reconstructed'. !!!")
print("!!!         Ray then spams 'suppressing duplicate error message' until interrupted !!!")
print("!!!         Ray may also spam 'ObjectID already exists in the object store' !!!")
print("!!!     df[df.COLUMN_NAME > 0]: Causes next line to spam and hang forever? May hang here. !!!")
print("!!!     df[df['COLUMN_NAME'] > 0]: Causes next line to spam for 2 seconds then recover. But may hang here !!!")

ray_df2 = None
if runToCompletion: 
  print("!!!     Skipping this for now to guarantee run to completion of this script !!!")
  printTimer(total_t, True, True)
else:
  start_t = datetime.datetime.now()

  # Always causes next step to spam/hang? May hang here too
  # ray_df2 = ray_df[(ray_df.tip_amount > 0) & (ray_df.fare_amount > 0)] 

  # Alternative code- next step recovers from hanging if do this, but may hang here
  ray_df2 = ray_df[(ray_df['tip_amount'] > 0) & (ray_df['fare_amount'] > 0)]

  total_t = datetime.datetime.now() - start_t
  result = ray_df2.head(topN)
  printRes(result, True)
  printTimer(total_t, True)

pd_df2 = None
if runPandas:
  start_t = datetime.datetime.now()
  pd_df2 = pd_df[(pd_df.tip_amount > 0) & (pd_df.fare_amount > 0)]
  total_t = datetime.datetime.now() - start_t
  result = pd_df2.head(topN)
  printRes(result, False)
  printTimer(total_t, False)


# Make new column
print('08: Profiling df2["new_column"] = df2.column / df2.column')
print('--------------------------------------------------')
print("!!! Ray always spams complaints for at least 2 seconds at this line. May be due to SettingWithCopyWarning raised by Pandas !!!")
print("!!!         Ray gives error 'task is a driver task and objects created by ray.put could not be reconstructed'. !!!")
print("!!!         Ray may spam ValueError: 'cannot reindex from a duplicate axis' at dataframe/dataframe.insert_col_part value, allow_duplicates!!!")
print("!!!         Ray may then spam 'Failed to get ObjectID as argument for remote function dataframe.utils.FUNCTION_CALLING_INSERT_COL_PART' !!!")
print("!!!     df['COLUMN_NAME'] = df.COLUMN_NAME / ...: Ray spams/hangs forever !!!")
print("!!!     df['COLUMN_NAME'] = df['COLUMN_NAME'] / ...: Ray MAY recover in 2 seconds. MAY return incorrect NaN value-- tip_fraction row 5 !!!")

if runToCompletion:
  print("!!!     Skipping this for now to guarantee run to completion of this script !!!")
  printTimer(total_t, True, True)
else:
  start_t = datetime.datetime.now()

  # Always hangs?
  # ray_df2["tip_fraction"] = ray_df2.tip_amount / ray_df2.fare_amount 

  # Alternative Code
  ray_df2["tip_fraction"] = ray_df2['tip_amount'] / ray_df2['fare_amount']

  total_t = datetime.datetime.now() - start_t
  result = ray_df2.head(topN)
  printRes(result, True)
  printTimer(total_t, True)

if runPandas:
  print("!!! Pandas's df2['tip_fraction'] = ... raises SettingWithCopyWarning that can be ignored !!!")
  print("!!! This is correct Pandas syntax for setting a new column !!!")
  start_t = datetime.datetime.now()
  pd_df2["tip_fraction"] = pd_df2.tip_amount / pd_df2.fare_amount
  total_t = datetime.datetime.now() - start_t
  result = pd_df2.head(topN)
  printRes(result, False)
  printTimer(total_t, False)


# Group by then mean on new column
print('09: Profiling twice df2.groupby(column).new_column.mean()')
print('--------------------------------------------------')
print("!!! Ray may spam complaints and/or hang at this line. !!!")
print("!!!         Ray may spam ValueError: 'cannot reindex from a duplicate axis' at dataframe/dataframe.insert_col_part value, allow_duplicates!!!")
print("!!!         Ray may then spam 'Failed to get ObjectID as argument for remote function dataframe.utils.FUNCTION_CALLING_INSERT_COL_PART' !!!")
print("!!!         Ray then spams 'suppressing duplicate error message' until interrupted !!!")
print("!!!     df.groupby(df.COLUMN_NAME).COLUMN_NAME.mean(): DataFrame AttributeError  !!!")
print("!!!     df.groupby(df.COLUMN_NAME)['COLUMN_NAME'].mean(): Not yet tried !!!")
print("!!!     df.groupby(df['COLUMN_NAME']).COLUMN_NAME.mean(): Not yet tried !!!")
print("!!!     df.groupby(df['COLUMN_NAME']['COLUMN_NAME'].mean(): Spams/hangs. Possible ValueError, depending on previous parts !!!")

if runToCompletion: 
  print("!!! Skipping for now as this part is dependent on previous new column and toDateTIme !!!")
  printTimer(total_t, True, True)
else:
  start_t = datetime.datetime.now()
  
  # day_of_week = (ray_df2.groupby(ray_df2.tpep_pickup_datetime.dt.dayofweek)['tip_fraction'].mean()) 
  # hour = (ray_df2.groupby(ray_df2.tpep_pickup_datetime.dt.hour)['tip_fraction'].mean())
  
  # Alternative code 1 - possible Ray ValueError, depending on previous parts. Or spams/hangs
  day_of_week = (ray_df2.groupby(ray_df2['tpep_pickup_datetime'].dt.dayofweek)['tip_fraction'].mean()) 
  hour = (ray_df2.groupby(ray_df2['tpep_pickup_datetime'].dt.hour)['tip_fraction'].mean())
  
  # Alternative code 2 - DataFrame AttributeError
  # day_of_week = (ray_df2.groupby(ray_df2.tpep_pickup_datetime.dt.dayofweek).tip_fraction.mean()) 
  # hour = (ray_df2.groupby(ray_df2['tpep_pickup_datetime'].dt.hour)['tip_fraction'].mean())
  
  # Alternative code 3 
  # day_of_week = (ray_df2.groupby(ray_df2['tpep_pickup_datetime'].dt.dayofweek).tip_fraction.mean()) 
  # hour = (ray_df2.groupby(ray_df2['tpep_pickup_datetime'].dt.hour)['tip_fraction'].mean())
  
  total_t = datetime.datetime.now() - start_t
  result = hour
  printRes(result, True)
  printTimer(total_t, True)

if runPandas:
  start_t = datetime.datetime.now()
  day_of_week = (pd_df2.groupby(pd_df2.tpep_pickup_datetime.dt.dayofweek)['tip_fraction'].mean())
  hour = (pd_df2.groupby(pd_df2.tpep_pickup_datetime.dt.hour)['tip_fraction'].mean())
  total_t = datetime.datetime.now() - start_t
  result = hour
  printRes(result, False)
  printTimer(total_t, False)


############## JOINS AND CORRELATIONS #################
print('############# SECTION: JOINS AND CORRELATIONS #############')
print('')

# Creating series
print('10: Profiling payments = DataFrame({...})')
print('--------------------------------------------------')
start_t = datetime.datetime.now()
ray_payments = pd.DataFrame({'num':[1, 2, 3, 4, 5, 6], 
              'payment_method':['Credit Card', 'Cash', 'No Charge', 
                              'Dispute', 'Unknown', 'Voided trip']})
total_t = datetime.datetime.now() - start_t
result = ray_payments
printRes(result, True)
printTimer(total_t, True)

pd_payments = None
if runPandas:
  start_t = datetime.datetime.now()
  pd_payments = old_pd.DataFrame({'num':[1, 2, 3, 4, 5, 6], 
                'payment_name':['Credit Card', 'Cash', 'No Charge', 
                                'Dispute', 'Unknown', 'Voided trip']})
  total_t = datetime.datetime.now() - start_t
  result = pd_payments
  printRes(result, False)
  printTimer(total_t, False)


# Joining
print('11: Profiling df2 = df.merge(payments, ...)')
print('--------------------------------------------------')
print("!!! Ray may spam/hang complaints at this line. !!!")
print("!!!     Ray gives error 'task is a driver task and objects created by ray.put could not be reconstructed'. !!!")
print("!!!     Ray spams error 'ObjectID already exists in the object store' until interrupted")

ray_df2 = None
if runToCompletion:
  print("!!!     Skipping this for now to guarantee run to completion of this script !!!")
  printTimer(total_t, True, True)
else:
  start_t = datetime.datetime.now()
  ray_df2 = ray_df.merge(ray_payments, left_on="payment_type", right_on="num")
  total_t = datetime.datetime.now() - start_t
  result = ray_df2.head(topN)
  printRes(result, True)
  printTimer(total_t, True)

pd_df2 = None
if runPandas:
  start_t = datetime.datetime.now()
  pd_df2 = pd_df.merge(pd_payments, left_on="payment_type", right_on="num")
  total_t = datetime.datetime.now() - start_t
  result = pd_df2.head(topN)
  printRes(result, False)
  printTimer(total_t, False)


# Groupby column on join
print('12: Profiling df2_groupby = df2.groupby(df2.column)')
print('--------------------------------------------------')
ray2_groupby = None
if runToCompletion:
  print("!!! Skipping for now as this part is dependent on previous merge !!!")
  printTimer(total_t, True, True)
else:
  start_t = datetime.datetime.now()
  ray2_groupby = ray_df2.groupby(ray_df2.payment_name)
  total_t = datetime.datetime.now() - start_t
  result = ray2_groupby.head(topGroupN)
  printRes(result, True)
  printTimer(total_t, True)

pd2_groupby = None
if runPandas:
  start_t = datetime.datetime.now()
  pd2_groupby = pd_df2.groupby(pd_df2.payment_name)
  total_t = datetime.datetime.now() - start_t
  result = pd2_groupby.head(topGroupN)
  printRes(result, False)
  printTimer(total_t, False)


# Calculate mean on groupby on join
print('13: Profiling df2_groupby.column.mean()')
print('--------------------------------------------------')
if runToCompletion:
  print("!!! Skipping for now as this part is dependent on previous merge !!!")
  printTimer(total_t, True, True)
else:
  start_t = datetime.datetime.now()
  result = ray2_groupby.tip_amount.mean()
  total_t = datetime.datetime.now() - start_t
  printRes(result, True)
  printTimer(total_t, True)

if runPandas:
  start_t = datetime.datetime.now()
  result = pd2_groupby.tip_amount.mean()
  total_t = datetime.datetime.now() - start_t
  printRes(result, False)
  printTimer(total_t, False)


# Calculate two where booleans on join
print('14: Profiling bool1 = df2.column == 0; bool2 = df2.column == value')
print('--------------------------------------------------')
ray_bool1 = None 
ray_bool2 = None
if runToCompletion:
  print("!!! Skipping for now as this part is dependent on previous merge !!!")
  printTimer(total_t, True, True)
else: 
  start_t = datetime.datetime.now()
  ray_bool1 = ray_df2.tip_amount == 0
  ray_bool2 = ray_df2.payment_name == 'Cash'
  total_t = datetime.datetime.now() - start_t
  result = ray_bool1.head(topN)
  printRes(result, True)
  printTimer(total_t, True)

pd_bool1 = None
pd_bool2 = None
if runPandas:
  start_t = datetime.datetime.now()
  pd_bool1 = pd_df2.tip_amount == 0
  pd_bool2 = pd_df2.payment_name == 'Cash'
  total_t = datetime.datetime.now() - start_t
  result = pd_bool1.head(topN)
  printRes(result, False)
  printTimer(total_t, False)

# Calculate concatenation of two booleans of join
print('15: Profiling bools = concat([bool1, bool2], axis=1) ')
print('--------------------------------------------------')
ray_bools = None
if runToCompletion:
  print("!!! Skipping for now as this part is dependent on previous merge !!!")
  printTimer(total_t, True, True)
else:
  start_t = datetime.datetime.now()
  ray_bools = pd.concat([ray_bool1, ray_bool2], axis=1)
  total_t = datetime.datetime.now() - start_t
  result = ray_bools.head(topN)
  printRes(result, True)
  printTimer(total_t, True)

pd_bools = None
if runPandas:
  start_t = datetime.datetime.now()
  pd_bools = old_pd.concat([pd_bool1, pd_bool2], axis=1)
  total_t = datetime.datetime.now() - start_t
  result = pd_bools.head(topN)
  printRes(result, False)
  printTimer(total_t, False)


# Calculate correlation on boolean concatenation of join
print('16: Profiling bools.corr()')
print('--------------------------------------------------')
if runToCompletion:
  print("!!! Skipping for now as this part is dependent on previous merge !!!")
  printTimer(total_t, True, True)
else:
  start_t = datetime.datetime.now()
  result = ray_bools.corr()
  total_t = datetime.datetime.now() - start_t
  printRes(result, True)
  printTimer(total_t, True)

if runPandas:
  start_t = datetime.datetime.now()
  result = pd_bools.corr()
  total_t = datetime.datetime.now() - start_t
  printRes(result, False)
  printTimer(total_t, False)



############## SHUFFLES AND TIME SERIES #################
print('############# SECTION: SHUFFLES AND TIME SERIES #############')
print('')

# (re)set index on df
print('17: Profiling df.set_index(column)')
print('--------------------------------------------------')
print("!!! Ray may spam until is interrupted here !!!")
if runToCompletion:
  print("!!!     Skipping this for now to guarantee run to completion of this script !!!")
  printTimer(total_t, True, True)
else:
  start_t = datetime.datetime.now()
  ray_df = ray_df.set_index('tpep_pickup_datetime')  # Remember to disable set_index in earlier DateTime conversion
  total_t = datetime.datetime.now() - start_t
  result = ray_df.head(topN)
  printRes(result, True)
  printTimer(total_t, True)

if runPandas:
  start_t = datetime.datetime.now()
  pd_df = pd_df.set_index('tpep_pickup_datetime')
  total_t = datetime.datetime.now() - start_t
  result = pd_df.head(topN)
  printRes(result, False)
  printTimer(total_t, False)


# Get head and tail of df (re)set by index
print('18: Profiling df.head() and df.tail()')
print('--------------------------------------------------')
print("!!! For some reason, Ray may spam at this simple and independent call until is interrupted here !!!")

if runToCompletion:
  print("!!! Skipping this even though this call is not dependent on previous !!!")
  printTimer(total_t, True, True)
else:
  start_t = datetime.datetime.now()
  ray_df.head()
  result = ray_df.tail()
  total_t = datetime.datetime.now() - start_t
  printRes(result, True)
  printTimer(total_t, True)

if runPandas:
  start_t = datetime.datetime.now()
  pd_df.head()
  result = pd_df.tail()
  total_t = datetime.datetime.now() - start_t
  printRes(result, False)
  printTimer(total_t, False)


# Get loc (re)set by index
print('19: Profiling df_loc = df.loc[index]')
print('--------------------------------------------------')
ray_loc = None
if runToCompletion:
  print("!!! Skipping this as is dependent on previous setIndex !!!")
  printTimer(total_t, True, True)
else:
  start_t = datetime.datetime.now()
  ray_loc = ray_df.loc['2015-01-10 19:01:44']
  total_t = datetime.datetime.now() - start_t
  printTimer(total_t, True)

pd_loc = None
if runPandas:
  start_t = datetime.datetime.now()
  pd_loc = pd_df.loc['2015-01-10 19:01:44']
  total_t = datetime.datetime.now() - start_t
  printTimer(total_t, False)

# Get head of loc (re)set by index
print('20: Profiling df_loc.head()')
print('--------------------------------------------------')
if runToCompletion:
  print("!!! Skipping this as is dependent on previous loc !!!")
  printTimer(total_t, True, True)
else:
  start_t = datetime.datetime.now()
  result = ray_loc.head()
  total_t = datetime.datetime.now() - start_t
  printRes(result, True)
  printTimer(total_t, True)

if runPandas:
  start_t = datetime.datetime.now()
  result = pd_loc.head()
  total_t = datetime.datetime.now() - start_t
  printRes(result, False)
  printTimer(total_t, False)



############## TIME SERIES #################
print('############# SECTION: TIME SERIES #############')
print('')

# Resample by day
print('21: Profiling df_resamp = df.column.resample("1d")')
print('--------------------------------------------------')
ray_resamp = None
if runToCompletion:
  print("!!! Skipping for now as this part is dependent on previous set_index and to_datetime !!!")
  printTimer(total_t, True, True)
else:
  start_t = datetime.datetime.now()
  ray_resamp = ray_df.passenger_count.resample('1d')
  total_t = datetime.datetime.now() - start_t
  printTimer(total_t, True)

pd_resamp = None
if runPandas:
  start_t = datetime.datetime.now()
  pd_resamp = pd_df.passenger_count.resample('1d')
  total_t = datetime.datetime.now() - start_t
  printTimer(total_t, False)


# Compute mean of resample
print('22: Profiling df_mn = df_resamp.mean()')
print('--------------------------------------------------')
ray_mn = None
if runToCompletion:
  print("!!! Skipping for now as this part is dependent on previous !!!")
  printTimer(total_t, True, True)
else:
  start_t = datetime.datetime.now()
  ray_mn = ray_resamp.mean()
  total_t = datetime.datetime.now() - start_t
  result = ray_mn
  printRes(result, True)
  printTimer(total_t, True)

pd_mn = None
if runPandas:
  start_t = datetime.datetime.now()
  pd_mn = pd_resamp.mean()
  total_t = datetime.datetime.now() - start_t
  result = pd_mn
  printRes(result, False)
  printTimer(total_t, False)


# Plot mean of resample
print('23: Profiling df_mn.plot()')
print('--------------------------------------------------')
print("!!! Skipping for now as plotting doesn't seem to be installed. !!!")
print("!!!     Can't import PyQt5 inside matplotlib !!!")
if runToCompletion or True:  # Always skip
  print("!!! Skipping for now as this part is dependent on previous !!!")
  printTimer(total_t, True, True)
else: 
  start_t = datetime.datetime.now()
  ray_mn.plot()
  total_t = datetime.datetime.now() - start_t
  printTimer(total_t, True)

if runPandas and False:  # Always skip
  start_t = datetime.datetime.now()
  pd_mn.plot()
  total_t = datetime.datetime.now() - start_t
  printTimer(total_t, False)
print('')


# Rolling aggregation
print('24: Profiling df_roll = df.column.rolling(10)')
print('--------------------------------------------------')
start_t = datetime.datetime.now()
ray_roll = ray_df.passenger_count.rolling(10)
total_t = datetime.datetime.now() - start_t
result = ray_roll
printRes(result, True)
printTimer(total_t, True)

pd_roll = None
if runPandas:
  start_t = datetime.datetime.now()
  pd_roll = pd_df.passenger_count.rolling(10)
  total_t = datetime.datetime.now() - start_t
  result = pd_roll
  printRes(result, False)
  printTimer(total_t, False)


# Compute mean on rolling aggregation
print('25: Profiling df_roll.mean()')
print('--------------------------------------------------')
start_t = datetime.datetime.now()
result = ray_roll.mean()
total_t = datetime.datetime.now() - start_t
printRes(result, True)
printTimer(total_t, True)

if runPandas:
  start_t = datetime.datetime.now()
  result = pd_roll.mean()
  total_t = datetime.datetime.now() - start_t
  printRes(result, False)
  printTimer(total_t, False)



############### END OF EXAMPLE #################################
print('############# END OF EXAMPLE #######################')
print('For the total example, Ray took (hh:mm:ss.ms) {}'.format(ray_session_t))
print('For the total example, Pandas took (hh:mm:ss.ms) {}'.format(pd_session_t))
