from datetime import datetime
import pandas as pd
import preprocessing as pre
from importlib import reload
reload(pre)
from datetime import date
import numpy as np



##########################################
# parsed data from MTA API
weeks_df = pre.load_data_by_date( start_date = date( 2013, 1, 5 ) , parsed_weeks = 2)

##########################################
# process parsed data and saved into csv
processed_weeks_df = load_and_process_data_from_mta_web( weeks_df)
processed_weeks_df.to_csv("data/2013_2014_new.csv",mode='a')



def load_and_process_data_from_mta_web( weeks_df):
    total_record_dict_per_id_per_date = [ ]

    for single_week in weeks_df:
        # processed week data to parallel format
        processed_week_df = pre.preprocess_data_parallel_rows_to_vertical(single_week)
        #processed_week_df = pre.preprocess_data_parallel_rows_to_vertical(two_week[0])

        # add helper columns
        processed_week_df = pre.create_helper_columns( processed_week_df)

        unique_dates = processed_week_df['DATE'].unique()
        unique_hours = processed_week_df.loc[ (processed_week_df[ 'DESC' ] == "REGULAR")  ]['TIME'].unique()
        processed_week_df.sort_values( 'TIME', ascending= True)

        print(unique_dates)


        for single_date in unique_dates:

            # set time - morning and night
            print( single_date )


            per_day_df = processed_week_df[ processed_week_df[ "DATE" ] == single_date ]
            #print(per_day_df)

            id_station_list = per_day_df["id_station"].unique()
            #print(id_station_list)
            for id_station in id_station_list:
                per_day_per_turntile_id_df = per_day_df.loc[  (per_day_df[ "id_station" ] == id_station) ].sort_values("TIME", ascending= True)
                #per_day_per_turntile_id_df.reset_index(inplace=True)
                early_time_entre = min(per_day_per_turntile_id_df["ENTRIES"])
                end_time_entre = max(per_day_per_turntile_id_df["ENTRIES"])
                early_time_exit = min(per_day_per_turntile_id_df["EXITS"])
                end_time_exit = max(per_day_per_turntile_id_df["EXITS"])
                #single_date = 1

                total_record_dict_per_id_per_date.append([single_date, id_station,early_time_entre, end_time_entre, early_time_exit, end_time_exit])
                 ##########################################################
    df = pd.DataFrame( total_record_dict_per_id_per_date,
                       columns=[ 'date', 'id_station', 'min_entre', "max_entre", 'min_exit', "max_exit" ] )

    return df


def single_week_processed( single_week ):
    total_record_dict_per_id_per_date = [ ]

    # processed week data to parallel format
    processed_week_df = pre.preprocess_data_parallel_rows_to_vertical( single_week )
    # processed_week_df = pre.preprocess_data_parallel_rows_to_vertical(two_week[0])

    # add helper columns
    processed_week_df = pre.create_helper_columns( processed_week_df )

    unique_dates = processed_week_df[ 'DATE' ].unique( )
    unique_hours = processed_week_df.loc[ (processed_week_df[ 'DESC' ] == "REGULAR") ][ 'TIME' ].unique( )
    processed_week_df.sort_values( 'TIME', ascending=True )

    print( unique_dates )

    for single_date in unique_dates:

        print( single_date )


        per_day_df = processed_week_df[ processed_week_df[ "DATE" ] == single_date ]
        # print(per_day_df)

        id_station_list = per_day_df[ "id_station" ].unique( )
        # print(id_station_list)
        for id_station in id_station_list:
            per_day_per_turntile_id_df = per_day_df.loc[ (per_day_df[ "id_station" ] == id_station) ].sort_values(
                "TIME", ascending=True )
            # per_day_per_turntile_id_df.reset_index(inplace=True)
            early_time_entre = min( per_day_per_turntile_id_df[ "ENTRIES" ] )
            end_time_entre = max( per_day_per_turntile_id_df[ "ENTRIES" ] )
            early_time_exit = min( per_day_per_turntile_id_df[ "EXITS" ] )
            end_time_exit = max( per_day_per_turntile_id_df[ "EXITS" ] )
            # single_date = 1

            total_record_dict_per_id_per_date.append(
                [ single_date, id_station, early_time_entre, end_time_entre, early_time_exit, end_time_exit ] )

    df = pd.DataFrame( total_record_dict_per_id_per_date,
                       columns=[ 'date', 'id_station', 'min_entre', "max_entre", 'min_exit', "max_exit" ] )


    return df



def get_2013_q1_row_counts( start_date_c = date( 2013, 1, 5 ), parsed_weeks_c=15):
    weeks_df = pre.load_data_by_date( start_date= start_date_c, parsed_weeks= parsed_weeks_c )
    ##########################################
    per_day_record = [ ]
    ##########################################
    for single_week in weeks_df:
        # processed week data to parallel format
        processed_week_df = pre.preprocess_data_parallel_rows_to_vertical( single_week )
        # add helper columns
        processed_week_df = pre.create_helper_columns( processed_week_df )

        unique_dates = processed_week_df[ 'DATE' ].unique( )

        print( unique_dates )
        ############
        for single_date in unique_dates:
            print( single_date )
            per_day_df = processed_week_df[ processed_week_df[ "DATE" ] == single_date ]
            print( len( per_day_df ) )
            per_day_record.append( [ single_date, len( per_day_df ) ] )
    ##########################################################
    df = pd.DataFrame( per_day_record, columns=[ 'date', 'total_rows' ] )
    df.to_csv( "data/daily_row_counts_q1.csv" )

    return df






