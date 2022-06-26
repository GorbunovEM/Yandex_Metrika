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

