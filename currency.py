import sys
import os
import time
import datetime
import json
import pandas as pd
import sqlite3
from pandas.io.json import json_normalize
import os.path
from os import path, stat
from datetime import datetime, timedelta, date
import requests

"""
currency class:
Features:
1. Can create historic currency data based on ANY currency and any date.
2. If object creation is a new object, it will create historic data with regarding dates.
3. If there is DB file with the parameter in the object creations, it will check missing dates and request the missing dates from API and insert to regarding DB table file
3. we can calculate average rate of currency for any time interval. If requested time is not in DB program will try find and insert them to DB from API
4. we can request last rate of currency, if it is not in DB file again program will understand it and try to get lastest data from API
5. while creating and object, if here will be no parameter. Default base currency is EUR and data will be for last 2 years. And this will take 3-5 minutes time

"""


# noinspection PyTypeChecker
class currency():

    def date_validate(self, v_date):
        try:
            datetime.strptime(v_date, '%Y-%m-%d')
        except ValueError:
            sys.exit("Incorrect data format, check your dates , they should be like: YYYY-MM-DD")

    def append_df(self, df, response_text):
        parsed = json.loads(response_text)  # parsing
        df_tmp = pd.DataFrame.from_dict(json_normalize(parsed), orient='columns')  # converting df
        return df.append(df_tmp, sort=False)  # appending final df

    def __init__(self, base_curr=None, start_date=None, end_date=None):
        if base_curr is None:
            self.base_curr = 'EUR'
        else:
            self.base_curr = base_curr

        if start_date is None:
            self.start_date = date.today() - timedelta(days=730)  # going back 2 years.
        else:
            self.start_date = datetime.strptime(start_date, '%Y-%m-%d').date()

        if end_date is None:
            self.end_date = date.today()
        else:
            self.end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

        self.main_url = 'https://api.exchangeratesapi.io/'
        self.symbols = '&symbols=USD,GBP,TRY,AUD,EUR,JPY,CAD,CHF,SEK,PLN'
        self.symbols = self.symbols.replace(',' + self.base_curr, '')
        num_symbols = len(self.symbols.replace('&symbols=', '').split(','))
        self.db_file = 'curr_db_' + self.base_curr + '.sqlite'



        if os.path.exists(self.db_file) and os.path.getsize(self.db_file) > 0:
            conn = sqlite3.connect(self.db_file)
            c = conn.cursor()
            df_col_names, df_col_num = self.get_col_names('df')
            if num_symbols+2 == df_col_num: #if there is no change in symbols data
                query = 'select max("date") as max_date, min("date") as min_date from df'
                c.execute(query)
                row = c.fetchone()
                if row[0] < self.end_date.strftime('%Y-%m-%d'):  # check max date
                    print("inserting required new data... " + row[0] + " " + self.end_date.strftime('%Y-%m-%d'))
                    self.insert_new_data(row[0])
                if row[1] > self.start_date.strftime('%Y-%m-%d'):  # check min date
                    print("inserting required old data... " + row[1] + " " + self.start_date.strftime('%Y-%m-%d'))
                    self.insert_old_data(row[1])
            else: #if there is a change in symbols data, re create history
                self.create_first_history()
        else: #if there is no valid DB file, create history
            self.create_first_history()


    def create_first_history(self):
        day_count = (self.end_date - self.start_date).days
        df = pd.DataFrame()
        print("Creating First History for " + self.base_curr + " based data please wait." + " (" + self.start_date.strftime('%Y-%m-%d') + "-" + self.end_date.strftime('%Y-%m-%d') + ")")
        for i in range(0, day_count):
            v_date = self.end_date - timedelta(days=i)
            v_date = v_date.strftime('%Y-%m-%d')
            df = self.df_request(df, v_date)

        conn = sqlite3.connect(self.db_file)
        df.to_sql('df', conn, index=False, if_exists='replace')
        print('{} based currency table created'.format(self.base_curr))


    def insert_new_data(self, begin_date):
        day_count = (datetime.today() - datetime.strptime(begin_date,'%Y-%m-%d')).days  # calculate number of days required to insert after start_date
        df2 = pd.DataFrame()
        print (day_count)
        if day_count > 0:
            for i in range(0, day_count):

                v_date = date.today() - timedelta(days=i)
                v_date = v_date.strftime('%Y-%m-%d')
                print (v_date)
                df2 = self.df_request(df2, v_date)

            print (df2)
            conn = sqlite3.connect(self.db_file)
            df2.to_sql('df2', conn, index=False, if_exists='replace')

            df_col_names, df_col_num = self.get_col_names('df')
            df2_col_names, df2_col_num = self.get_col_names('df2')

            if df_col_num < df2_col_num:
                query = 'insert into df(' + df_col_names + ') select ' + df_col_names + ' from df2'
            else:
                query = 'insert into df(' + df2_col_names + ') select ' + df2_col_names + ' from df2'

            c = conn.cursor()
            c.execute(query)
            conn.commit()

    def insert_old_data(self, begin_date):
        day_count = (datetime.strptime(begin_date,'%Y-%m-%d').date() - self.start_date).days  # calculate number of days required to insert before start_date
        df2 = pd.DataFrame()
        if day_count > 0:
            for i in range(0, day_count):

                v_date = self.start_date + timedelta(days=i)
                v_date = v_date.strftime('%Y-%m-%d')
                df2 = self.df_request(df2, v_date)

            conn = sqlite3.connect(self.db_file)
            df2.to_sql('df2', conn, index=False, if_exists='replace')

            df_col_names, df_col_num = self.get_col_names('df')
            df2_col_names, df2_col_num = self.get_col_names('df2')

            if df_col_num < df2_col_num:
                query = 'insert into df(' + df_col_names + ') select ' + df_col_names + ' from df2'
            else:
                query = 'insert into df(' + df2_col_names + ') select ' + df2_col_names + ' from df2'
            c = conn.cursor()
            c.execute(query)


            conn.commit()

    def calculate_avg(self, curr_code, start_date, end_date):
        self.date_validate(start_date)
        self.date_validate(end_date)

        if os.path.exists(self.db_file):
            conn = sqlite3.connect(self.db_file)
            query = 'select max("date") as max_date, min("date") as min_date from df'
            c = conn.cursor()
            c.execute(query)
            row = c.fetchone()
            if row[0] < end_date:  # check max date
                print("inserting required new data... between " + row[0] + " " + end_date)
                self.insert_new_data(row[0])
            if row[1] > start_date:  # check min date
                print("inserting required old data... between " + start_date + ' ' + row[1])
                day_count = (self.start_date - datetime.strptime(start_date,'%Y-%m-%d').date()).days  # calculate number of days required to insert before start_date
                df2 = pd.DataFrame()
                if day_count > 0:
                    for i in range(0, day_count):
                        v_date = datetime.strptime(start_date, '%Y-%m-%d') + timedelta(days=i)
                        v_date = v_date.strftime('%Y-%m-%d')
                        df2 = self.df_request(df2, v_date)

                    conn = sqlite3.connect(self.db_file)
                    df2.to_sql('df2', conn, index=False, if_exists='replace')

                    df_col_names, df_col_num = self.get_col_names('df')
                    df2_col_names, df2_col_num = self.get_col_names('df2')

                    if df_col_num<df2_col_num:
                        query = 'insert into df('+df_col_names+') select '+df_col_names+' from df2'
                    else:
                        query = 'insert into df(' + df2_col_names + ') select ' + df2_col_names + ' from df2'

                    c = conn.cursor()
                    c.execute(query)
                    conn.commit()

            query = 'select "' + self.base_curr + '", avg("rates.' + curr_code + '") as avg_' + curr_code + ' from df where "date" between "' + start_date + '" and "' + end_date + '" group by "' + self.base_curr + '"'

            res = pd.read_sql(query, conn)
            avg_value = str(res['avg_' + curr_code].values[0])

            print("1 " + self.base_curr + " is average " + avg_value + ' ' + curr_code + " in between " + start_date + " and " + end_date)
            # of course we can return this like a function.



        else:
            sys.exit("please first check DB file or re-create currency Object")

    def get_last_rate(self, curr_code):

        if os.path.exists(self.db_file):
            conn = sqlite3.connect(self.db_file)
            query = 'select max("date") as max_date from df'
            c = conn.cursor()
            c.execute(query)
            row = c.fetchone()
            if row[0] < date.today().strftime('%Y-%m-%d'):  # check max date
                self.insert_new_data(date.today().strftime('%Y-%m-%d'))
            query = 'select "' + self.base_curr + '", "rates.' + curr_code + '" as ' + curr_code + ', "date" from df order by "date" desc limit 1'

            res = pd.read_sql(query, conn)
            last_value = str(res[curr_code].values[0])

            print('On date {}, 1 {} is {} {}'.format(res['date'].values[0], self.base_curr, last_value, curr_code))
            # of course we can return this like a function and use for calculations.

    def df_request(self, df, v_date):


        url = self.main_url + v_date + '?base=' + self.base_curr+self.symbols
        response = requests.get(url)  # requesting data
        df = self.append_df(df, response.text)
        return df

    def get_col_names(self, table_name):
        conn = sqlite3.connect(self.db_file)
        cursor = conn.execute('select * from '+table_name+' limit 1')
        col_names = list(map(lambda x: x[0], cursor.description))
        str_col = ""
        for col in range(0, len(col_names)):
            if col == 0:
                str_col = '"'+col_names[col]+'"'
            else:
                str_col = str_col+', '+'"'+col_names[col]+'"'
        return str_col, len(col_names)

def main():
    # here some example runs.

    curr_obj_eur = currency('EUR', '2019-01-01')  # creating a currency EUR based object with the dates in the parameters.
    curr_obj_eur.calculate_avg('USD', '2019-04-01', '2019-06-21')  # base currency EUR average of USD
    curr_obj_eur.get_last_rate('AUD')  # prints last rate of EUR - AUD

    curr_obj_usd = currency('USD', '2019-04-01')  # creating a currency USA based object with the dates in the parameters.
    curr_obj_usd.calculate_avg('EUR', '2019-02-01', '2019-06-21')  # base currency USD and print average of EUR
    curr_obj_usd.get_last_rate('TRY')  # prints last rate of USD - TRY

    curr_obj_try = currency('TRY')  # creating a currency TRY based object with out date, it will create for 2 years.
    curr_obj_try.calculate_avg('JPY', '2019-04-01', '2019-06-21')  # base currency TRY and print average of JPY
    curr_obj_try.calculate_avg('EUR', '2019-04-01', '2019-06-21')  # base currency TRY and print average of EUR



if __name__ == '__main__':
    main()
