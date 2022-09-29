from Metrika import YaMetrika, LoopingError, CreationQueryError
import time
import json
import datetime
from to_Database import Fill_Database


def create_obj(TOKEN, COUNTER_ID, START_DATE, END_DATE):
    report = YaMetrika(TOKEN, COUNTER_ID, START_DATE, END_DATE)
    return report


def eval(type ,report):
    if report.eval_query(type):
        created_responce = report.creating_query(type)
        request_id = json.loads(created_responce.text)['log_request']['request_id']
        status, processed_responce = report.checking_status(request_id, type)
        return status, processed_responce, request_id
    else:
        return CreationQueryError('Page is not available')


def check_readiness(status, report, request_id, type):
    TIME_TO_SLEEP = 10
    LOOP_ERROR_N = 50
    i = 0
    while status == 'created':
        time.sleep(TIME_TO_SLEEP)
        status, processed_responce = report.checking_status(request_id, type)
        i += 1
        if i > LOOP_ERROR_N:
            raise LoopingError('request_id:{0}'.format(request_id))
    return status, processed_responce, request_id

def final(processed_responce, report, request_id, type):
    parts = json.loads(processed_responce.text)['log_request']['parts']
    df = report.download(request_id, parts, type)
    return df


if __name__ == '__main__':

    TOKEN = 'XXXXXXXXXXXXXXXXXXXXXXXXXXXX'
    COUNTER_ID = 44147844
    START_DATE = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    END_DATE = (datetime.datetime.today() - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    type = 'hits'
    #type = 'visits'

    DB_HOST = 'localhost'
    DB_NAME = 'testdb'
    DB_LOGIN = 'XXXXX'
    DB_PASS = 'XXXXX'
    DB_PORT = XXXX
    ssh_username = 'XXXXX'
    ssh_password = 'XXXXX'

    a = create_obj(TOKEN, COUNTER_ID, START_DATE, END_DATE)
    b = eval(type, a)
    c = check_readiness(b[0], a, b[2], type)
    d = final(c[1], a, c[2], type)

    sql = Fill_Database(DB_HOST, DB_NAME, DB_LOGIN, DB_PASS, DB_PORT)
    port = list(sql.start_ssh_tunnel(ssh_username, ssh_password).keys())[0][1]
    sql.create_engine(port)
    try:
        sql.to_sql(d, type)
        print("Success")
    except Exception as err:
        sql.mis(err)
        print("Not")

