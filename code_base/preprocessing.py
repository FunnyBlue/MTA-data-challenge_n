import pandas as pd
from datetime import datetime
import pandas as pd
from datetime import date, timedelta


def process_id_date_columns(df):

	df[ "date" ] = df[ "date" ].apply( lambda x: x[ :-2 ] + "20" + x[ -2: ] )
	df[ "date" ] = df[ "date" ].astype( str )
	#df[ "date" ] = df[ "date" ].apply( lambda x: datetime.strptime( x, '%m/%d/%Y' ) )
	df[ "date" ] = df[ "date" ].apply( lambda x: datetime.strptime( x, '%m-%d-%Y' ) )
	df[ "station_t" ] = df[ "id_station" ].apply( lambda x: x.split( "_" )[ 0 ] + "_" + x.split( "_" )[ 1 ] )
	df[ "remote_unit" ] = df[ "id_station" ].apply( lambda x: x.split( "_" )[ 1 ] )
	df[ "c/a" ] = df[ "id_station" ].apply( lambda x: x.split( "_" )[ 0 ] )
	df[ "total_entries" ] = df[ "max_entre" ] - df[ "min_entre" ]
	df[ "total_exits" ] = df[ "max_exit" ] - df[ "min_exit" ]
	df[ "total_entries" ] = df[ "total_entries" ].astype( float )
	df[ "total_exits" ] = df[ "total_exits" ].astype( float )
	df = df.sort_values( "date" )

	return df



def analyze_full_t(row):

    if row['full_cap_flag'] is True:
        result = 1
    else:
        result = 0

    return result

def analyze_close_t(row):

    if  row['close_flag'] is True:
        result = 1
    else:
        result = 0
    return result


def analyze_full_assign_value(row):

    if row['#_stations_full_cap'] >0:
        result = row['#_stations_full_cap']
    else:
        result = 0

    return result

def analyze_close_assign_value(row):

    if  row['#_stations_close'] >0:
        result =  row['#_stations_close']
    else:
        result = 0
    return result




def preprocess_data_parallel_rows_to_vertical(df):
	# create empty list
	df_list = []

	# column_1 for row identifier
	column_1 = [ 'C/A', 'UNIT', 'SCP' ]

	# column_2 for single row input
	column_2 = [ 'DATE', 'TIME', 'DESC', 'ENTRIES', 'EXITS' ]

	# add columns to new dataframe base on column_name
	for i in range( 8 ):
		column_name = [ 3 + i * 5 + j for j in range( 1, 6, 1 ) ]
		df_list.append( df[column_name] )

	# append column1 to row input
	df1 = df[ [ 1, 2, 3 ] ]
	df1.columns = column_1

	column_2 = [ 'DATE', 'TIME', 'DESC', 'ENTRIES', 'EXITS' ]
	result = [ ]
	for i in df_list:
		i.columns = column_2
		# parralel combined
		i = pd.concat( [ df1, i ], axis=1 )
		result.append( i )

	# vertical combined
	df_merge_t = pd.concat(
		[ result[ 0 ], result[ 1 ], result[ 2 ], result[ 3 ], result[ 4 ], result[ 5 ], result[ 6 ], result[ 7 ] ] )

	return df_merge_t

def create_helper_columns(df_merge_t):



	# create datetime object
	df_merge_t["datetime"] = df_merge_t["DATE"]+df_merge_t["TIME"]
	# drop na
	df_merge_t = df_merge_t.dropna(subset=['datetime'])
	# date formatting
	df_merge_t["datetime"] = df_merge_t["datetime"].apply(lambda x:datetime.strptime(x,'%m-%d-%y%H:%M:%S'))


	# create id from columns
	df_merge_t["id_station"] = df_merge_t["C/A"]+ "_" + df_merge_t["UNIT" ] + "_" + df_merge_t["SCP" ]

	return df_merge_t



def sanity_check_s (row):
    if row['min_entre'] <0 or  row['max_entre']<0 :
        return True
    elif row['min_exit'] <0 or row['max_exit'] <0:
        return True
    elif row['total_entries'] <  0 or row['total_exits'] <  0 :
        return True
    else:
        return False

def check_day(row):
	if row[ 'date' ] =='01-01-13':
		return True
	elif row[ 'date' ] =='12-31-13':
		return False



def weird_value_check (row):
	value = False
	if row[ 'min_entre' ]!= 0 and  row[ 'max_entre' ]!= 0:
		if row[ 'min_entre' ]>= 10000 and  row[ 'max_entre' ]>= 10000:
			if row[ 'total_entries' ] / row['min_entre'] > 0.5:
				value = True
				return value
		elif row[ 'min_exit' ]>= 10000 and  row[ 'max_exit' ]>= 10000:
			if row[ 'total_exits' ] / row['min_exit'] > 0.5:
				value = True
				return value

	if row[ 'min_entre' ] < 1 and row[ 'max_entre' ]!= 0:
		# per day total entry number is too big while the minimum entry on that day is 0
		if row[ 'total_entries' ] > 100000:
			value = True
			return value
	if row[ 'min_exit' ] < 1 and row[ 'max_exit' ] != 0:
		# per day total exit number is too big while the minimum exit on that day is 0
		if row[ 'total_exits' ] > 100000:
			value = True
			return value

	# per tunrtile device should not exceed 1M per day, which is almost impossible!
	if row[ 'total_exits'] > 1000000 or  row[ 'total_entries'] > 1000000:
		value = True
		return value

	return value






def procss_df_helper_columns_q4(df):
	df[ "station_t" ] = df[ "id_station" ].apply( lambda x: x.split( "_" )[ 0 ] + "_" + x.split( "_" )[ 1 ] )
	df[ "remote_unit" ] = df[ "id_station" ].apply( lambda x: x.split( "_" )[ 1 ] )
	df[ "c/a" ] = df[ "id_station" ].apply( lambda x: x.split( "_" )[ 0 ] )
	df[ "total_entries" ] = df[ "max_entre" ] - df[ "min_entre" ]
	df[ "total_exits" ] = df[ "max_exit" ] - df[ "min_exit" ]
	df[ "total_entries" ] = df[ "total_entries" ].astype( float )
	df[ "total_exits" ] = df[ "total_exits" ].astype( float )
	# df['day_dff'] = df[ "date" ].apply(lambda x: pre.check_day(x))

	df[ 'sanity_check' ] = df.apply( lambda x: sanity_check_s( x ), axis=1 )
	df[ 'weird_value_check' ] = df.apply( lambda x: weird_value_check( x ), axis=1 )

	return df



def check_full_cap(row):
	if row['busy'] >= (row['full_cap']*0.95):
		return True
	else:
		return False

def check_closed(row):
	if row['busy']== 0:
		return True
	else:
		return False