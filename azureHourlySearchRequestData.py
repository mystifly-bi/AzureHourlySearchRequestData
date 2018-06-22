
from azure.storage.table import TableService, Entity

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import datetime
import threading
import random, time
import os,sys,glob
from dateutil.relativedelta import relativedelta
import DatabaseHelper
conns = DatabaseHelper.create_database_connections()
ats_conns = DatabaseHelper.create_azure_connections()
db_engine_pm = conns['pm_reportdb']
db_engine_mfb = conns['mfb_reportdb']


global start_date,end_date, rep_date, MAX_THREAD_COUNT, DAYS_TO_DOWNLOAD, START_DATE_MONTH, START_DATE_YEAR, START_DATE_DAY
MAX_THREAD_COUNT = 100


def setup():
    DAYS_TO_DOWNLOAD = 1

    global table_service
    table_service = TableService(account_name='******', account_key='***************************************************************************')

    # Get a list of active clients. Will fail if not connected to VPN
    q_active_clients = '''
    SELECT clientId, clientName, AccountNumber FROM ClientProfileDetails
    WHERE Status = 'Active'
    ORDER BY clientId
    '''

    _=os.system('cls')
    print('Getting list of active clients')
    print('\nEnsure connection to VPN and \'Press Enter\'. ')

    global df_active_clients
    df_active_clients = pd.read_sql(q_active_clients, db_engine_pm)


    global start_date, rep_date, end_date
    start_date = datetime.datetime.today()
    start_date = datetime.datetime(start_date.year,start_date.month,start_date.day,start_date.hour) - datetime.timedelta(hours=6, minutes=30)

    rep_date = start_date

    end_date = start_date + datetime.timedelta(hours=1)
    print("start_date", start_date)
    print("end_date", end_date)
    start_delta = datetime.datetime.max - start_date
    global start_ticks
    start_ticks = start_delta.days * 24 * 60 * 60 * 10**7 + start_delta.seconds * 10**7

    end_delta = datetime.datetime.max - end_date
    global end_ticks
    end_ticks = end_delta.days * 24 * 60 * 60 * 10**7 + end_delta.seconds * 10**7
    print(start_date, end_date, start_ticks, end_ticks)
    assert(start_ticks > end_ticks)

#Global Threading lock for getting hourly data.

def get_prp_value(prp):
    return prp.value

def to_date(str_dt):
    str_dt = str(str_dt)
    if len(str_dt)>1:
        str_dt = str_dt.split(' ')[0]
    else:
        str_dt = ''
    return str_dt




def trip_type(str):
    try:
        if len(str) >=2:
            tType = ((str.split('|')[0]).split('-')[1]).strip()
            if '<br />' in tType:
                tType = tType.split('<br />')[0].strip()
            else:
                tType = tType

        else:
            tType = ''
        return tType
    except Exception as e:
        print('Exception')

def cabin_class(str):
    try:
        if len(str) >=2:
            if 'PremiumEconomy' in str:
                cab_class =  'PremiumEconomy'
            elif 'Business' in str:
                cab_class =  'Business'
            elif 'Economy' in str:
                cab_class =  'Economy'
            elif 'First' in str:
                cab_class =  'First'
            else:
                cab_class ='Economy'
        else:
            cab_class ='Economy'
        return cab_class
    except Exception as e:
        return 'Economy'





def flight_pref(str):
    try:
        if len(str) >=2:
            if "Airline Pref" not in str:
                fPrep = ''
            else:
                str =  (str.split('|')[-1])
                fPrep = str.split('-')[-1].strip()
        else:
            fPrep = ''
        return fPrep
    except Exception as e:
        return 'Exception'


def exflight_pref(str):
    try:
        if len(str) >=2:
            if "Exclude Airline" not in str:
                exPrep = ''
            else:
                str =  (str.split('|')[-1])
                exPrep = str.split('-')[-1].strip()
        else:
            exPrep = ''
        return exPrep
    except Exception as e:
        return 'Exception'


def travel_date(str):
    try:
        if len(str) >=2:
            start = '<br />'
            end = '|'
            tDate = (((str[str.find(start)+len(start):str.rfind(end)]).split('|')[0]).split('-')[0].strip())[:11]
        else:
            tDate =''
        return tDate
    except Exception as e:
        return 'Exception'

def return_date(str):
    try:
        if len(str) >=2:
            start = '<br />'
            end = '|'
            rDate = (((str[str.find(start)+len(start):str.rfind(end)]).split('|')[0]).split('-')[-1].strip())[:11]
        else:
            rDate =''
        return rDate
    except Exception as e:
        return 'Exception'

def adult(str):
    try:
        if len(str) >=2:
            str = (str.split('|')[2])
            if 'Adult' in str:
                index = str.index('Adult')
                sub_string = str[index-5:index+6]
                noOfAdult = [int(s) for s in sub_string.split() if s.isdigit()][0]
            else:
                noOfAdult = 0
        else:
            noOfAdult = 0
        return noOfAdult
    except Exception as e:
        return 'Exception'

def child(str):
    try:
        if len(str) >=2:
            str = (str.split('|')[2])
            if 'Child' in str:
                index = str.index('Child')
                sub_string = str[index-5:index+6]
                noOfChild = [int(s) for s in sub_string.split() if s.isdigit()][0]
            else:
                noOfChild = 0
        else:
            noOfChild = 0
        return noOfChild
    except Exception as e:
        return 0

def infant(str):
    try:
        if len(str) >=2:
            str = (str.split('|')[2])
            if 'Infant' in str:
                index = str.index('Infant')
                sub_string = str[index-5:index+6]
                noOfInfant = [int(s) for s in sub_string.split() if s.isdigit()][0]
            else:
                noOfChild = 0
        else:
            noOfInfant = 0
        return noOfInfant
    except Exception as e:
        return 0



def stopOver(str):
    try:
        if len(str) >=2:
            if 'Refundable' not in str:
                str = (str.split('|')[1])
                index = str.index('<br />')
                sub_string = str[0:index]
                sub_string = sub_string.strip()
            else:
                str = (str.split('|')[2])
                index = str.index('<br />')
                sub_string = str[0:index]
                sub_string = sub_string.strip()

        else:
            sub_string = ''
        return sub_string
    except Exception as e:
        return 'Exception'


def is_booked(bref):
    try:
        if str(bref) == '0' or len(str(bref)) <= 1:
            bstatus = 0
        else:
            bstatus = 1
        return bstatus
    except Exception as e:
        return 0



def convert_utc_to_ist(i_date_time):
    ist_time = datetime.datetime(year = i_date_time.year,
                                 month = i_date_time.month,
                                 day = i_date_time.day,
                                 hour = i_date_time.hour,
                                 minute = i_date_time.minute,
                                 second = i_date_time.second)
    return(ist_time + datetime.timedelta(hours=5, minutes=30))


def convert_utc_to_ist_hour(i_date_time):
    i_date_time = (i_date_time + relativedelta(hours=5, minutes=30) + relativedelta(minute=0, second=0, microsecond=0))
    ist_time = datetime.datetime(year = i_date_time.year,
                                 month = i_date_time.month,
                                 day = i_date_time.day,
                                 hour = i_date_time.hour,
                                 minute = 0,
                                 second = 0)
    return(ist_time)

def convert_datetime_to_date(i_date):
    ist_date = datetime.datetime(year = i_date.year,
                                 month = i_date.month,
                                 day = i_date.day)
    return(ist_date)

def get_data(clientId):

    global start_ticks, end_ticks, start_date, rep_date, end_date

    table_name = 'FlightSearchHistory'
    filter_string = "PartitionKey eq '{}' and RowKey lt '00000{}' and RowKey gt '00000{}'".format(clientId,start_ticks, end_ticks)
    selection = ['PartitionKey', 'RowKey','searchedOn', 'SearchDetails', 'BookingRef', 'TicketedStatus',
                 'clientId', 'clientName', 'memberId', 'memberName', 'origin', 'searchRequestId', 'destination',
                 'searchIdentifier', 'RowKey','travelDate'
                ]
    file_name = '{}_{}_searchdetails.csv'.format(end_date.date().isoformat(), clientId)

    searches = table_service.query_entities(table_name, filter=filter_string , select=','.join(selection), num_results=None)

    df = pd.DataFrame(searches.items)


    while searches.next_marker:
        searches = table_service.query_entities(table_name, filter=filter_string , select=','.join(selection), marker=searches.next_marker)
        df = df.append(pd.DataFrame(searches.items))

    if df.shape[0] > 0:
        df.BookingRef = df.BookingRef.apply(get_prp_value)
        df.clientId = df.clientId.apply(get_prp_value)
        df.memberId = df.memberId.apply(get_prp_value)
        df.searchRequestId = df.searchRequestId.apply(get_prp_value)
        df['searchedOnIST'] = df.searchedOn.apply(convert_utc_to_ist)
        df['searchedOnISTHour'] = df.searchedOn.apply(convert_utc_to_ist_hour)
        df['searchedDate'] = df.searchedOnIST.apply(convert_datetime_to_date)
        df['travelDateOnIST'] = df.travelDate.apply(convert_utc_to_ist)
        df['travelDate_calculated'] = df.travelDateOnIST.apply(convert_datetime_to_date)
        df.to_csv('daily_data/{}'.format(file_name), index=False)


def get_data_by_tickdiff(client_id, start_query_ticks, stop_query_ticks):
    global file_write_lock, all_search_data
    table_name = 'FlightSearchHistory'
    filter_string = "PartitionKey eq '{}' and RowKey lt '00000{}' and RowKey gt '00000{}'".format(client_id, start_query_ticks, stop_query_ticks)
    selection = ['PartitionKey', 'searchedOn', 'SearchDetails', 'BookingRef', 'TicketedStatus',
                 'clientId', 'clientName', 'memberId', 'memberName', 'origin', 'searchRequestId', 'destination',
                 'searchIdentifier', 'RowKey','travelDate'
                ]

    searches = table_service.query_entities(table_name, filter=filter_string , select=','.join(selection), num_results=None)

    df = pd.DataFrame(searches.items)


    while searches.next_marker:
        searches = table_service.query_entities(table_name, filter=filter_string , select=','.join(selection), marker=searches.next_marker)
        df = df.append(pd.DataFrame(searches.items))

    if df.shape[0] > 0:
        df.BookingRef = df.BookingRef.apply(get_prp_value)
        df.clientId = df.clientId.apply(get_prp_value)
        df.memberId = df.memberId.apply(get_prp_value)
        df.searchRequestId = df.searchRequestId.apply(get_prp_value)
        df['searchedOnIST'] = df.searchedOn.apply(convert_utc_to_ist)
        df['searchedOnISTHour'] = df.searchedOn.apply(convert_utc_to_ist_hour)
        df['searchedDate'] = df.searchedOnIST.apply(convert_datetime_to_date)
        df['travelDateOnIST'] = df.travelDate.apply(convert_utc_to_ist)
        df['travelDate_calculated'] = df.travelDateOnIST.apply(convert_datetime_to_date)

        with file_write_lock:
            if all_search_data.shape[0] == 0:
                all_search_data = df.copy()
            else:
                all_search_data = all_search_data.append(df)


def main():
    setup()

    print(datetime.datetime.today())

    threads_list = {}
    start_count = 0
    start_time = datetime.datetime.now()
    HOURS_TO_ADD = 10 #Minutes

    global file_write_lock, df_active_clients, all_search_data, start_ticks, end_ticks

    all_search_data = pd.DataFrame() # An empty dataframe where all data will be stored.

    file_write_lock = threading.Lock()


    for index, clientId in df_active_clients.clientId.iteritems():
        partial_end_ticks = start_ticks - (HOURS_TO_ADD * 60 * 10**7)
        #partial_end_ticks = max(partial_end_ticks, end_ticks)
        partial_start_ticks = start_ticks
        #print('{:024d} < '.format(partial_start_ticks))

        while partial_end_ticks >= end_ticks:
            #print('{:024d} - '.format(partial_end_ticks))
            #print('{:024d} > '.format(end_ticks))
            #partial_end_ticks -= int(HOURS_TO_ADD * 60 * 60* 10**7)
            #partial_end_ticks = max(partial_end_ticks, end_ticks-1)
            #continue

            while threading.active_count() >= MAX_THREAD_COUNT :
                _=os.system('cls')
                print('{:4d} threads started till [MCN{:06d}]'.format(start_count, int(max(threads_list.keys()).split('_')[0])))
                print(all_search_data.shape)
                print('Active Threads: ', threading.active_count())
                time.sleep(0.5)
                start_count = 0



            t = threading.Thread(target=get_data_by_tickdiff, args=(clientId,partial_start_ticks, partial_end_ticks))
            t.start()
            start_count += 1

            threads_list['{:06d}_{}'.format(clientId,partial_start_ticks)] = t

            partial_start_ticks = partial_end_ticks

            partial_end_ticks -= (HOURS_TO_ADD * 60 * 10**7)
            partial_end_ticks = max(partial_end_ticks, end_ticks-1)



        #if index > 1000:
        #    break


    active_threads = threading.active_count()
    while active_threads > 0:
        active_threads = 0
        _=os.system('cls')
        print('{} elapsed.'.format(datetime.datetime.now() - start_time))
        print(all_search_data.shape)
        for k,th in threads_list.copy().items():
            if th.is_alive():
                print('[MCN{:06d}][{}] is running'.format(int(k.split('_')[0]),k.split('_')[1]))
                active_threads += 1
                #th.join()
            else:
                del[threads_list[k]]
        if active_threads > 0:
            time.sleep(2)


    all_search_data['Trip'] = all_search_data.SearchDetails.apply(trip_type)
    all_search_data['CabinClass'] = all_search_data.SearchDetails.apply(cabin_class)
    # all_search_data['PreferredAirline'] = all_search_data.SearchDetails.apply(flight_pref)
    # all_search_data['ExcludeAirline'] = all_search_data.SearchDetails.apply(exflight_pref)
    # #all_search_data['TravelDate'] = all_search_data.SearchDetails.apply(travel_date)
    # all_search_data['ReturnDate'] = all_search_data.SearchDetails.apply(return_date)
    # all_search_data['NoOfAdult'] = all_search_data.SearchDetails.apply(adult)
    # all_search_data['NoOfChild'] = all_search_data.SearchDetails.apply(child)
    # all_search_data['NoOfInfant'] = all_search_data.SearchDetails.apply(infant)
    # all_search_data['StopOverDetails'] = all_search_data.SearchDetails.apply(stopOver)
    all_search_data['status'] = all_search_data.BookingRef.apply(is_booked)

    all_search_data.to_csv('D:/SearchData/daily_data/{}_searchdetails.csv'.format(str(start_date).replace(':','')), index=False)



    q_clients = '''
        SELECT clientId, AccountNumber FROM ClientProfileDetails
    '''
    #df_clients = pd.read_sql(q_clients, db_engine_pm2)
    df_clients = pd.read_sql(q_clients, db_engine_pm)

    df_merge = pd.merge(all_search_data, df_clients, on=['clientId'] , how='left')


    #print(df_merge.dtypes)

    df_final = df_merge.groupby([ 'searchedDate', 'searchedOnISTHour', 'AccountNumber', 'clientId', 'clientName' , 'origin', 'destination', 'CabinClass', 'Trip' ]).agg({'searchRequestId': 'count', 'status': np.sum})

    df_final = df_final.reset_index()
    df_final.rename(columns={'searchRequestId': 'SearchCount', 'status' : 'BookedCount'}, inplace=True)



    df_final = df_final[['searchedDate', 'AccountNumber', 'clientId', 'clientName', 'origin', 'destination','CabinClass','Trip','BookedCount', 'SearchCount', 'searchedOnISTHour']]
    df_final.to_csv('D:/SearchData/calculated/{}_agg_searchdetails.csv'.format(str(start_date).replace(':','')), index=False)

    try:
        connection = db_engine_pm.raw_connection()
        cursor = connection.cursor()

    except Exception as e:
        pass

    try:
        for index, row in df_final.iterrows():
            cursor.execute("insert into DailySearchDataTemp values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (row[0], row[1], row[2],row[3],row[4],row[5], row[6],row[7], float(row[8]), float(row[9]),row[10]) )
            cursor.commit()
            connection.close()
    except Exception as e:
        pass

    print(datetime.datetime.today())
if __name__ == "__main__":
    main()
