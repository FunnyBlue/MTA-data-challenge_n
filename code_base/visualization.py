import pandas as pd
import numpy as np
from datetime import datetime
import plotly.express as px
import preprocessing as pre
import plotly.graph_objects as go
from importlib import reload
reload(pre)
##########################################################################################
# q1: Plot the daily row counts for data files in Q1 2013
##########################################################################################

################### data preprocessing ############################

df = pd.read_csv("data/plot_q1_daily_row_counts.csv")
df["date"] = df["date"].apply(lambda x: x[:-2] + "20" + x[-2:])
df["date"] = df["date"].astype(str)
df["date"] = df["date"].apply(lambda x: datetime.strptime(x,'%m-%d-%Y'))
df = df.loc[(df["date"] >= datetime(2013,1,1)) & (df["date"] <= datetime(2013,3,31))]

#df.to_csv("data/output/plot/plot_q1_data.csv")

################### plot ############################

df["date"] = df["date"].astype(np.datetime64)
fig = px.histogram(df, x="date", y="total_rows",
                   nbins=len(df),
                   barmode="group",
                   height=800)

config = dict({'scrollZoom': True})
fig.update_layout(bargap=0.2)
fig.update_layout(
    yaxis_title='total rows per date',
    xaxis_title='q1 2013 date'
)
fig.show(config=config)
fig.write_html("plot_Q1.html")




##########################################################################################
# q2: Plot the daily total number of entries & exits across the system for Q1 2013.
##########################################################################################

################### data preprocessing ############################

dfa = pd.read_csv("data/2013_2014_new.csv",index_col=0)
dfa = pre.process_id_date_columns(dfa)

# # add helper columns
dfa[ 'sanity_check' ] = dfa.apply( lambda x: pre.sanity_check_s( x ), axis=1 )
dfa[ 'weird_value_check' ] = dfa.apply( lambda x: pre.weird_value_check( x ), axis=1 )
dfa = dfa.loc[( dfa["date"] >= "2013-01-01" ) & (dfa["date"] < "2014-01-01")]

dfa_clean  = dfa.loc[ (dfa[ 'sanity_check' ] != True ) & ( dfa[ 'weird_value_check' ] != True) ]
dfa_weird  = dfa.loc[ (dfa[ 'sanity_check' ] == True ) | ( dfa[ 'weird_value_check' ] == True) ]

dfa_clean[dfa_clean[ 'weird_value_check' ] == True]

s = dfa[ dfa[ 'weird_value_check' ] == True]
h = dfa[ dfa[ 'sanity_check' ] == True]

dfa_clean.to_csv("data/output/plot/per_turntile_stat_all_year_clean.csv")
dfa_weird.to_csv("data/output/plot/per_turntile_stat_all_year_weird_and_wrong_stat.csv")

# old dfa: 1711544 rows
################### read data ############################


dfa_clean = pd.read_csv("data/output/plot/per_turntile_stat_all_year_clean.csv",index_col=0)


# filter on q1 2013 only
dfa_q1_2013_clean = dfa_clean.loc[( dfa_clean["date"] >= "2013-01-01" ) & (dfa_clean["date"] < "2013-04-01")]

# dfa_q1_2013_by_date = dfa_q1_2013.groupby("date").sum()
# dfa_q1_2013_by_date.reset_index(inplace=True)
# dfa_q1_2013_by_date["date"] = dfa_q1_2013_by_date["date"].astype(np.datetime64)

################################ plot ################################
#dt = dfa_q1_2013_by_date["date"].tolist()
dt = dfa_q1_2013_clean["date"].tolist()
print(len(dt))
print(len(dfa_q1_2013_clean ["total_entries"]))
print(len(dfa_q1_2013_clean ["total_exits"]))

dfa_q1_2013_clean[ 'weird_value_check' ] = dfa_q1_2013_clean.apply( lambda x: pre.weird_value_check( x ), axis=1 )


fig = px.histogram(dfa_q1_2013_clean, x=dfa_q1_2013_clean["date"], y=["total_entries", "total_exits"],
                   barmode='group',
                   height=800)
config = dict({'scrollZoom': True})
fig.show(config=config)
fig.write_html("plot_Q2_t.html")

fig = px.histogram(dfa_q1_2013_clean, x=dfa_q1_2013_clean["date"], y=["total_entries", "total_exits"],
                   barmode='group',
                   height=800)
config = dict({'scrollZoom': True})
fig.show(config=config)
fig.write_html("plot_Q2_t.html")

##########################################################################################
# q3: Plot the mean and standard deviation of the daily total number of entries & exits for each month in Q1 2013 for station 34 ST-PENN STA
##########################################################################################

######################### preprocess data #########################
all_year_clean = pd.read_csv("data/output/plot/per_turntile_stat_all_year_clean.csv",index_col=0)
#all_year_clean[all_year_clean['weird_value_check']== True]
all_year_clean["busy"] = all_year_clean["total_entries"] + all_year_clean["total_exits"]

# connect station name with turntile device
remote_station_list = pd.read_csv('data/Remote-Booth-Station.csv')
remote_station_list.rename(columns = {'Remote': 'unit', 'Booth': 'c/a'}, inplace = True)
all_year_clean_with_station  = pd.merge(  all_year_clean ,remote_station_list ,  how='left', left_on=['remote_unit','c/a'], right_on = ['unit','c/a'])


per_station_per_day = all_year_clean_with_station .groupby(['Station','date']).sum()
per_station_per_day.reset_index( inplace=True)
per_station_per_day_q1_2013 = per_station_per_day.loc[( per_station_per_day["date"] >= "2013-01-01" ) & (per_station_per_day["date"] < "2013-04-01")]
pen_station_q1 = per_station_per_day_q1_2013[per_station_per_day_q1_2013['Station']=='34 ST-PENN STA']
pen_station_q1.to_csv("data/output/plot/pen_station_q1.csv")

################################ plot ################################

per_date_total_sum = pen_station_q1.groupby("date").sum()
per_date_total_sum.reset_index(inplace=True)
per_date_total_sum["date"] = per_date_total_sum["date"].apply(lambda x: datetime.strptime(x,'%Y-%m-%d'))

per_date_total_sum["month"] = per_date_total_sum["date"].dt.month
per_month_mean_std_by_day = per_date_total_sum.groupby("month").agg(['mean', 'std'])

per_month_mean_std_by_day = per_month_mean_std_by_day[ [('total_entries', 'mean'),('total_entries',  'std'),('total_exits', 'mean'),('total_exits',  'std') ]]
per_month_mean_std_by_day.to_csv("data/output/plot/q3_mean_std_data.csv")


print(per_month_mean_std_by_day )

fig = go.Figure()
fig.add_trace(go.Bar(
    name='Entries',
    x=['Jan', 'Feb', 'Mar'], y=per_month_mean_std_by_day["total_entries"]["mean"].tolist(),
    error_y=dict(type='data', array=per_month_mean_std_by_day["total_entries"]["std"].tolist())
))
fig.add_trace(go.Bar(
    name='Exits',
    x=['Jan', 'Feb', 'Mar'], y=per_month_mean_std_by_day["total_exits"]["mean"].tolist(),
    error_y=dict(type='data', array=per_month_mean_std_by_day["total_entries"]["std"].tolist())
))
fig.update_layout(barmode='group')
config = dict({'scrollZoom': True})
fig.show(config=config)
fig.write_html("plot_Q3.html")


##########################################################################################
# q4: Plot 25/50/75 percentile of the daily total number of entries & exits for each month in Q1 2013 for station 34 ST-PENN STA.
##########################################################################################


pen_station_q1 = pd.read_csv("data/output/plot/pen_station_q1.csv")
per_date_total_sum = pen_station_q1.groupby("date").sum()
per_date_total_sum.reset_index(inplace=True)
per_date_total_sum["date"] = per_date_total_sum["date"].apply(lambda x: datetime.strptime(x,'%Y-%m-%d'))
per_date_total_sum["month"] = per_date_total_sum["date"].dt.month
################################ plot ################################


x = per_date_total_sum['month'].tolist()


fig = go.Figure()

fig.add_trace(go.Box(
    y= per_date_total_sum["total_entries"].tolist(),
    x=x,
    name='Entries',
    marker_color='#3D9970'
))
fig.add_trace(go.Box(
    y=per_date_total_sum["total_exits"].tolist(),
    x=x,
    name='Exits',
    marker_color='#FF4136'
))


fig.update_layout(
    yaxis_title= 'percentile plot',
    xaxis_title='month',

    boxmode='group' # group together boxes of the different traces for each value of x
)
config = dict({'scrollZoom': True})
fig.show(config=config)
fig.write_html("plot_Q4.html")


############################################################################################################
# q5: Plot the daily number of closed stations and number of stations that were not operating at full capacity in Q1 2013.
##########################################################################################
stat = pd.read_csv("data/output/analyze/q5.2_full_close_station_list_2013.csv")

################################ data preprocessing ################################
stat["date"] = stat["date"].apply(lambda x: datetime.strptime(x,'%Y-%m-%d'))
# from full year to q1 2013anity_check
stat = stat.loc[( stat["date"] >= "2013-01-01" ) & (stat ["date"] < "2013-04-01")]
stat['full_cap_count'] = stat.apply(lambda x: pre.analyze_full_t(x), axis = 1)
stat['close_count'] = stat.apply(lambda x: pre.analyze_close_t(x), axis = 1)

per_date_total_stations_reach_full_close = stat.groupby("date").sum()
per_date_total_stations_reach_full_close.rename(columns = {'full_cap_count': '#_stations_full_cap', 'close_count': '#_stations_close'}, inplace = True)


# create empty dataframe to hold station numbers that are either full or closed per date

per_date = pd.DataFrame()

dates_in_q1 = pd.date_range(start="2013-01-01",end="2013-03-31")
per_date['date'] = dates_in_q1
per_date['full_station_number'] = 0
per_date['close_station_number'] = 0

f = pd.merge( per_date, per_date_total_stations_reach_full_close ,  how='left', left_on=['date'], right_on = ['date'])

per_date['full_station_number'] = f.apply(lambda x: pre.analyze_full_assign_value(x), axis = 1)
per_date['close_station_number'] = f.apply(lambda x: pre.analyze_close_assign_value(x), axis = 1)

per_date.to_csv("data/output/plot/plot_q5_full_close_station_number.csv")

################################ plot ################################
per_date = pd.read_csv("data/output/plot/plot_q5_full_close_station_number.csv")
per_date["date"] = per_date["date"].astype(np.datetime64)
fig = px.histogram(per_date, x="date", y=["full_station_number", "close_station_number"],
                   nbins=len(per_date),
                   barmode="group",
                   height=800)

config = dict({'scrollZoom': True})

fig.update_layout(
    yaxis_title='# of station close or reach full capacity',
    xaxis_title='q1 2013 date'
)
fig.show(config=config)
fig.write_html("plot_Q5_t.html")