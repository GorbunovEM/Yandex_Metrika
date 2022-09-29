import pandas as pd
from sqlalchemy import create_engine
import psycopg2
from sshtunnel import SSHTunnelForwarder


class Fill_Database():

    def __init__(self, DB_HOST, DB_NAME, DB_LOGIN, DB_PASS, DB_PORT):
        self.DB_HOST = DB_HOST
        self.DB_NAME = DB_NAME
        self.__DB_LOGIN = DB_LOGIN
        self.__DB_PASS = DB_PASS
        self.DB_PORT = DB_PORT

    def start_ssh_tunnel(self, ssh_username, ssh_password):
        server = SSHTunnelForwarder(
            ('151.248.114.190', 22),
        # ssh_private_key="</path/to/private/ssh/key>",
        ### in my case, I used a password instead of a private key
            ssh_username=ssh_username,
            ssh_password=ssh_password,
            remote_bind_address=('127.0.0.1', self.DB_PORT))
        server.start()
        server.check_tunnels()
        print(server.tunnel_is_up, flush=True)
        return server.tunnel_is_up

    def create_engine(self, port):
        self.engine = create_engine(f'postgresql+psycopg2://{self.__DB_LOGIN}:{self.__DB_PASS}@{self.DB_HOST}:{port}/{self.DB_NAME}')


    def to_sql(self, df, TABLE_NAME):
        if not df.empty:
            df.to_sql(TABLE_NAME, con=self.engine, index=False, if_exists='append')
            self.engine.dispose()
        else:
            pass

    def mis(self, err):
        pd.DataFrame({
            'message_': [err],
            'is_ok': 0
        }, columns=['message', 'is_ok']).to_sql('message_mis', con=self.engine, if_exists='append', index=False)
        self.engine.dispose()

