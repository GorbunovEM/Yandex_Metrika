import requests
import sys
import psycopg2
import sqlalchemy
import sshtunnel
from sshtunnel import SSHTunnelForwarder
from psycopg2 import Error
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import paramiko
import io
import pandas as pd
from sqlalchemy import create_engine

# create SSH connection
server = SSHTunnelForwarder(
            ('XXX.XXX.XXX.XXX', XXX),
            # ssh_private_key="</path/to/private/ssh/key>",
            ### in my case, I used a password instead of a private key
            ssh_username="XXXX",
            ssh_password="XXXX",
            remote_bind_address=('127.0.0.1', 5432))
            
server.start()
server.check_tunnels()
print(server.tunnel_is_up, flush=True)

# create connection to database
params = {
            'database': 'testdb',
            'user': 'XXXXX',
            'password': 'XXXX',
            'host': 'localhost',
            'port': server.local_bind_port
        }
conn = psycopg2.connect(**params)
curs = conn.cursor()

#check connection 
sql_tables = 'select * from pg_catalog.pg_tables;'
curs.execute(sql_tables)
results = curs.fetchall()
print(results)

#create dataframe with data from Yandex Metrika
payload = {
    'metrics': 'ym:s:visits, ym:s:pageviews, ym:s:users, ym:s:avgVisitDurationSeconds, ym:s:mobilePercentage',
    'dimensions' : 'ym:s:date, ym:s:referer',
    'date1': '30daysAgo',
    'date2': 'today',
    'limit': 10000,
    'ids': XXXXXX,
    'accuracy': 'full',
    'pretty': True,
    'oauth_token': 'XXXXXXX'
}
r = requests.get('https://api-metrika.yandex.ru/stat/v1/data', params=payload)
print(r)
data = r.json()

df = pd.DataFrame(columns=['date', 'reffer', 'visits', 'pageviews', 'users', 'avgVisitDurationSeconds', 'mobilePercentage'])
i = len(data['data']) - 1
while i > 0:
    new_row = {'date':data['data'][i]['dimensions'][0]['name'], 
               'reffer':data['data'][i]['dimensions'][1]['name'], 
               'visits':data['data'][i]['metrics'][0], 
               'pageviews':data['data'][i]['metrics'][1],
               'users':data['data'][i]['metrics'][2],
               'avgVisitDurationSeconds':data['data'][i]['metrics'][3],
               'mobilePercentage':data['data'][i]['metrics'][4]}
    df = df.append(new_row, ignore_index=True)
    i = i - 1
new_df = df.sort_values(by=['date'], ascending=False)

#insert data into sql table
conn2 = create_engine(f'postgresql+psycopg2://user_name:password@localhost:{server.local_bind_port}/testdb')
new_df.to_sql('metrika', con=conn2, index=False, if_exists= 'replace')
alter_table = '''alter table metrika add id_numb serial'''
curs.execute(alter_table)

#check table
query = '''select * from metrika;'''
record = curs.fetchone()
print(record)

#finsh
conn.commit()
conn.close()
conn2.dispose()
server.stop()
