import json
import psycopg2
import pyodbc
import pandas as pd
import datetime
from datetime import date
from io import StringIO


def conn_psy(bd_type, stand_name, conn_file):

    with open(conn_file, 'r') as f:
        data = json.loads(f.read())
        conn_param = data[bd_type][stand_name]
        conn_str = ''

        if bd_type == 'GP':
            conn_str = data['CONNSTR'][bd_type][conn_param['conntype']]['psycopg2']
            conn_str = conn_str.format(**conn_param)
            conn = psycopg2.connect(conn_str)
            conn.set_client_encoding('UNICODE')
            return conn

        else:
            raise Exception('Only postgres connections')


file = r'С:\conn.json'
datastore = 'Krasnogorsk_data'

try:
    pg_conn = conn_psy('PG', 'MVP_MAIN', file)
    pg_conn.set_client_encoding('UNICODE')

except:
    print ("Ошибка подключения к БД !")

pyodbc.pooling=False



def trimmer (dframe):
    cols = dframe.columns
    for i in range (len(cols)):
        dframe[cols[i]] = dframe[cols[i]].astype(str).str.replace (" ","")


def duplidel (dframe):  
    dframe.drop_duplicates(inplace = True, keep='last')


def numfix (dframe):
    dframe ['profit']= dframe['profit'].astype(str).str.replace (",",".")
  


curdate = date.today()
cdate = curdate.strftime("%d.%m.%Y")
ct = datetime.datetime.now()
buffio = StringIO()



try:
    datacsv = pd.read_csv(datastore+cdate+'.csv', sep = ';', dtype = object)

except:
    pg_conn = conn_psy('PG', 'MVP_MAIN', file)
    sql_log = """
                select public.p_retnet_log ('Попытка чтения', 'Данные не поступили """+datastore+"""');
                """
    with pg_conn.cursor() as curs:
        curs.execute (sql_log)
        pg_conn.commit()
    pg_conn.close()
    
                
datacsv = datacsv.assign(store_id =pd.Series())
datacsv = datacsv.assign(load_date =pd.Series())

duplidel(datacsv)
trimmer (datacsv)
numfix (datacsv)

datacsv ['store_id'] = '1'
datacsv ['load_date'] = ct

datacsv.to_csv(buffio, mode = 'a', sep = ';', encoding = 'utf-8',index = False, header = False)
buffio.seek(0)
    


try:
    pg_conn = conn_psy('PG', 'MVP_MAIN', file)

    with pg_conn.cursor() as curs:
        curs.copy_from(buffio,'public.tmp_sales', sep = ';', null = 'nan')
        pg_conn.commit()
    
    sql_log = """
                select public.p_retnet_log ('Попытка записи', 'Успешно !"""+datastore+"""');
                """

    with pg_conn.cursor() as curs:
        curs.execute (sql_log)
        pg_conn.commit()

    pg_conn.close()
    datacsv.drop (datacsv.index, inplace = True)
    print("Успешно !")

except:
    pg_conn = conn_psy('PG', 'MVP_MAIN', file)
    sql_log = """
                select public.p_retnet_log ('Попытка записи', 'Невозможно произвести запись !"""+datastore+"""');
                """
    with pg_conn.cursor() as curs:
        curs.execute (sql_log)
        pg_conn.commit()
    pg_conn.close()

    
pg_conn.close()