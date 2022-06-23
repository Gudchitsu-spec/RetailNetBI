import json
import psycopg2
import pyodbc

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


try:
    pg_conn = conn_psy('PG', 'MVP_MAIN', file)
    pg_conn.set_client_encoding('UNICODE')

except:
    print ("Ошибка подключения к БД !")

pyodbc.pooling=False


sortsqlfrod = '''
insert into public.frod_sales( 
select * from public.tmp_sales 
where 
transaction_id is null or
item_id is null or
amount is null or
profit is null or
transaction_date is null
);

delete from public.tmp_sales 
where 
transaction_id is null or
item_id is null or
amount is null or
profit is null or
transaction_date is null;
'''

sortsqlclear = '''
insert into public.t_sales( 
select * from public.tmp_sales 
);
'''

sorttrunc = '''
delete from public.tmp_sales;
'''
try:
    pg_conn = conn_psy('PG', 'MVP_MAIN', file)

    sql_log = """
        select public.p_retnet_log ('Очистка от NULL', 'Успешно !');
        """

    with pg_conn.cursor() as curs:
        curs.execute (sortsqlfrod)
        curs.execute (sortsqlclear)
        curs.execute (sql_log)
        pg_conn.commit()

    pg_conn.close()

except:
    pg_conn = conn_psy('PG', 'MVP_MAIN', file)

    sql_log = """
        select public.p_retnet_log ('Очистка от NULL', 'Ошибка !');
        """

    with pg_conn.cursor() as curs:
        curs.execute (sql_log)
        pg_conn.commit()

    pg_conn.close()
    
    
pg_conn.close()