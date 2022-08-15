from datetime import datetime
import pandas as pd
import preprocessing as pre
from importlib import reload
reload(pre)
from datetime import date
import numpy as np

################################################################################################
# q1- Which station has the most number of units?
################################################################################################
remote_station_list = pd.read_csv('data/Remote-Booth-Station.csv')
remote_station_list.rename(columns = {'Remote': 'unit', 'Booth': 'c/a'}, inplace = True)

remote_station_count = remote_station_list.groupby("Station").count()

print( "q1- Which station has the most number of units? ")
print("ans is: " + str(remote_station_count['unit'].idxmax()))
print("it has # of units: " + str( max(remote_station_count['unit'])))

################################################################################################
# q2 - What is the total number of entries & exits across the subway system for February 1, 2013?
################################################################################################

per_day_per_turnstile_id_clean = pd.read_csv("data/output/plot/per_turntile_stat_all_year_clean.csv",index_col=0)

per_day_per_turnstile_id_weird = pd.read_csv("data/output/plot/per_turntile_stat_all_year_weird_and_wrong_stat.csv",index_col=0)

#per_day_per_turnstile_id = pre.process_id_date_columns(per_day_per_turnstile_id)
feb_1_2013 = per_day_per_turnstile_id_clean[ per_day_per_turnstile_id_clean['date'] =="2013-02-01"]

feb_1_2013[ 'sanity_check' ] = feb_1_2013.apply( lambda x: pre.sanity_check_s( x ), axis=1 )
feb_1_2013[ 'weird_value_check' ] = feb_1_2013.apply( lambda x: pre.weird_value_check( x ), axis=1 )
feb_1_2013_normal_df = feb_1_2013.loc[ (feb_1_2013[ 'sanity_check' ] != True ) & ( feb_1_2013[ 'weird_value_check' ] != True) ]



print( "q2 - What is the total number of entries & exits across the subway system for February 1, 2013? ")
print("total entries is: " + str( feb_1_2013_normal_df['total_entries'].sum()  ))
print("total exits is: " + str( feb_1_2013_normal_df['total_exits'].sum()  ))
#print("the weird turntile values are listed below and exclused from counting: \n " + str( total_weird_list_q2))


################################################################################################
# q3 - Let’s define the busy-ness as sum of entry & exit count.
################################################################################################

feb_1_2013_normal_df["busy"] = feb_1_2013_normal_df["total_entries"] + feb_1_2013_normal_df["total_exits"]
################################################################################################
# 3.1 What station was the busiest on February 1, 2013?
################################################################################################


feb_1_2013_busy = pd.merge( feb_1_2013_normal_df, remote_station_list ,  how='left', left_on=['remote_unit','c/a'], right_on = ['unit','c/a'])

feb_1_2013_busy_per_station = feb_1_2013_busy .groupby("Station").sum()

feb_1_2013_busy_per_station['busy'].idxmax()
max(feb_1_2013_busy_per_station['busy'])



print( "q3.1: What station was the busiest on February 1, 2013?")
print("most busy station is : " + str( feb_1_2013_busy_per_station['busy'].idxmax() ))
print("total sum of entries and exits is: " + str( max(feb_1_2013_busy_per_station['busy'])  ))

################################################################################################
# 3.2 What turnstile was the busiest on that date?
################################################################################################

feb_1_2013_normal_df.set_index( "id_station", inplace = True)

turntile_id = feb_1_2013_normal_df["busy"].idxmax()
turntile_id_busy_value = max(feb_1_2013_normal_df["busy"])


print( "q3.2 -  What turnstile was the busiest on that date? ")
print("the most busy turntile id is : " + str( turntile_id) )
print("the sum of entries and exits of that turntile is: " + str( turntile_id_busy_value  ))

################################################################################################
# q4: - What stations have seen the most usage growth/decline in 2013?
################################################################################################

############# load data from csv (saved from api) ###################################

per_day_per_turnstile_id_clean = pd.read_csv("data/output/plot/per_turntile_stat_all_year_clean.csv",index_col=0)
per_day_per_turnstile_id_clean= per_day_per_turnstile_id_clean.loc[( per_day_per_turnstile_id_clean["date"] >= "2013-01-01" ) & (per_day_per_turnstile_id_clean["date"] <= "2013-12-31")]

####################################################################################


first_day = per_day_per_turnstile_id_clean[per_day_per_turnstile_id_clean ['date']=='2013-01-01']
first_day['check_day'] = True
last_day = per_day_per_turnstile_id_clean [per_day_per_turnstile_id_clean ['date']=='2013-12-31']
last_day['check_day'] = False

df = pd.concat([first_day, last_day], axis =0)



normal_df = df.loc[ (df[ 'sanity_check' ] != True ) & ( df[ 'weird_value_check' ] != True) ]

#df_2013_busy = pd.merge( normal_df, remote_station_list ,  how='left', left_on=['remote_unit','c/a'], right_on = ['unit','c/a'])

normal_df["busy"] = normal_df["total_entries"] + normal_df["total_exits"]
# first_day = normal_df[normal_df['date']=='01-01-13']
# last_day = normal_df[normal_df['date']=='12-31-13']
#
# first_day  = pd.merge( [first_day ,remote_station_list ],  how='left', left_on=['remote_unit','c/a'], right_on = ['unit','c/a'])
# last_day  = pd.merge( [last_day ,remote_station_list ],  how='left', left_on=['remote_unit','c/a'], right_on = ['unit','c/a'])
all_day  = pd.merge( normal_df ,remote_station_list ,  how='left', left_on=['remote_unit','c/a'], right_on = ['unit','c/a'])


a = all_day[all_day['date']=='2013-01-01'].groupby('Station').sum()
b = all_day[all_day['date']=='2013-12-31'].groupby('Station').sum()
c = pd.merge(a, b, left_index=True, right_index=True)
c.rename(columns = {'busy_x': 'first_day_2013', 'busy_y': 'last_day_2013'}, inplace = True)


c['growth_rate'] = (c['last_day_2013'] / c['first_day_2013']) -1
c['grow_number'] = c['last_day_2013'] -  c['first_day_2013']

station_name = c['growth_rate'].idxmax()
station_growth_max = max(c['growth_rate'])

station_name_num = c['grow_number'].idxmax()
station_growth_max_num = max(c['grow_number'])



print( "q4: - What stations have seen the most usage growth/decline in 2013? ")
print("the highest growth rate station is : " + str( station_name) )
print("the rate is: " + str( station_growth_max  ))
print("the highest growth station (sum of entries+exits)  station is : " + str( station_name_num) )
print("the total visit number growth is: " + str( station_growth_max_num ))



########################################################################################################
# q5.1: What dates are the least busy?
# q5.2:Could you identify days on which stations were not operating at full capacity or closed entirely?
########################################################################################################


################################################################################################
# q5.1: What dates are the least busy?
################################################################################################
################ read from previous data preprocessing part ################
all_year_clean = pd.read_csv("data/output/plot/per_turntile_stat_all_year_clean.csv",index_col=0)
#all_year_clean[all_year_clean['weird_value_check']== True]
all_year_clean["busy"] = all_year_clean["total_entries"] + all_year_clean["total_exits"]
################################################################################
all_year_per_day = all_year_clean.groupby("date").sum()
all_year_per_day['busy'].idxmin()
all_year_per_day.sort_values("busy",ascending=True,inplace=True)
all_year_per_day.reset_index(inplace=True)
least_busy_top_10_df = all_year_per_day[0:10][['date','busy']]

least_busy_top_10_df.sort_values("busy",inplace=True)
least_busy_top_10_df.to_csv("data/output/analyze/q5.1_the least busy 10 days in 2013.csv")



print( "q5.1: - What dates are the least busy? ")
print("the least busy 10 days in 2013 are: \n "  + str(least_busy_top_10_df ))



##########################################################################################################
# q5.2: Could you identify days on which stations were not operating at full capacity or closed entirely?
############################################################################################################
all_year_clean = pd.read_csv("data/output/plot/per_turntile_stat_all_year_clean.csv",index_col=0)
#all_year_clean[all_year_clean['weird_value_check']== True]
all_year_clean["busy"] = all_year_clean["total_entries"] + all_year_clean["total_exits"]
all_year_clean_with_station  = pd.merge(  all_year_clean ,remote_station_list ,  how='left', left_on=['remote_unit','c/a'], right_on = ['unit','c/a'])
###################################
per_station_per_day = all_year_clean_with_station.groupby(['Station','date']).sum()
per_station_per_day.reset_index( inplace=True)
# get every station's highest sum of entry+exits per date as full capacity threshold
all_year_station_list = per_station_per_day['Station'].unique()
get_full_capacity = []
for station in all_year_station_list:
	#print(station)
	full_capacity_per_station = max(per_station_per_day[per_station_per_day['Station']==station]['busy'])
	get_full_capacity.append([station,full_capacity_per_station ])

full_cap_list = pd.DataFrame(get_full_capacity, columns=['station','full_cap'])
##########################################################################################
# based on every station's full_cap value, map it to each station per date row
full_cap_df_per_station  = pd.merge( per_station_per_day ,full_cap_list ,  how='left', left_on=['Station'], right_on = ['station'])
full_cap_df_per_station["full_cap_flag"] = full_cap_df_per_station.apply(lambda x: pre.check_full_cap(x), axis=1)
full_cap_df_per_station["close_flag"] = full_cap_df_per_station.apply(lambda x: pre.check_closed(x), axis = 1)

# get lists of full_cap station and closed station rows

station_full_or_close_per_date = full_cap_df_per_station.loc[ (full_cap_df_per_station[ 'full_cap_flag' ] == True ) | ( full_cap_df_per_station[ 'close_flag' ] == True) ]

station_full_or_close_per_date = station_full_or_close_per_date[["date","Station","full_cap_flag","close_flag"]]
station_full_or_close_per_date.sort_values("date",inplace=True, ascending=True)
dates = station_full_or_close_per_date['date'].unique()
station_full_or_close_per_date.to_csv("data/output/analyze/q5.2_full_close_station_list_2013.csv")

print( "q5.2: Could you identify days on which stations were not operating at full capacity or closed entirely?")
print("the list of stations closed or reach full capacity per date are: \n "  + str(station_full_or_close_per_date))



