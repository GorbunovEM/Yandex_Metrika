import requests
import pandas as pd
import json
from io import StringIO


class CreationQueryError(Exception): pass

class LoopingError(Exception): pass

class YaMetrika():

    API_HOST = 'https://api-metrika.yandex.ru'
    SOURCE_pv = 'hits'
    API_FIELDS_pv = ('ym:pv:watchID', 'ym:pv:counterID', 'ym:pv:dateTime', 'ym:pv:referer', 'ym:pv:browser',
                  'ym:pv:browserLanguage', 'ym:pv:deviceCategory', 'ym:pv:mobilePhone', 'ym:pv:regionCity',
                  'ym:pv:regionCountry', 'ym:pv:screenColors', 'ym:pv:screenFormat', 'ym:pv:windowClientHeight',
                  'ym:pv:windowClientWidth', 'ym:pv:clientID', 'ym:pv:ipAddress')
    SOURCE_s = 'visits'
    API_FIELDS_s = ('ym:s:visitID', 'ym:s:counterID', 'ym:s:dateTime', 'ym:s:visitDuration', 'ym:s:regionCity',
                    'ym:s:regionCountry', 'ym:s:clientID', 'ym:s:referer', 'ym:s:browserLanguage',
                    'ym:s:deviceCategory', 'ym:s:mobilePhone', 'ym:s:browser', 'ym:s:screenFormat', 'ym:s:screenColors',
                    'ym:s:windowClientWidth', 'ym:s:windowClientHeight')


    def __init__(self, TOKEN, COUNTER_ID, START_DATE, END_DATE):
        self.__TOKEN = TOKEN
        self.__COUNTER_ID = COUNTER_ID
        self.START_DATE = START_DATE
        self.END_DATE = END_DATE

        self.params_hits = {
            'date1': self.START_DATE,
            'date2': self.END_DATE,
            'source': self.SOURCE_pv,
            'fields': ','.join(sorted(self.API_FIELDS_pv, key=lambda s: s.lower()))
        }

        self.params_visits = {
            'date1': self.START_DATE,
            'date2': self.END_DATE,
            'source': self.SOURCE_s,
            'fields': ','.join(sorted(self.API_FIELDS_s, key=lambda s: s.lower()))
        }

    def eval_query(self, source):
        url = '{host}/management/v1/counter/{counter_id}/logrequests/evaluate' \
            .format(
            host=self.API_HOST,
            counter_id=self.__COUNTER_ID
        )
        headers = {'Authorization': 'OAuth ' + self.__TOKEN}

        if source == 'hits':
            responce = requests.get(url, params=self.params_hits, headers=headers)
        elif source == 'visits':
            responce = requests.get(url, params=self.params_visits, headers=headers)


        if responce.status_code == 200:
            return json.loads(responce.text)['log_request_evaluation']['possible']
        else:
            return False

    def creating_query(self, source):
        url = '{host}/management/v1/counter/{counter_id}/logrequests' \
                .format(host=self.API_HOST,
                 counter_id=self.__COUNTER_ID)
        headers = {'Authorization': 'OAuth ' + self.__TOKEN}

        if source == 'hits':
            responce = requests.post(url, params=self.params_hits, headers=headers)
        elif source == 'visits':
            responce = requests.post(url, params=self.params_visits, headers=headers)

        if (responce.status_code == 200) and json.loads(responce.text)['log_request']:
            return responce
        else:
            raise CreationQueryError(responce.text)

    def checking_status(self, request_id, source):
        url = '{host}/management/v1/counter/{counter_id}/logrequest/{request_id}' \
            .format(request_id=request_id,
                    counter_id=self.__COUNTER_ID,
                    host=self.API_HOST)
        headers = {'Authorization': 'OAuth ' + self.__TOKEN}

        if source == 'hits':
            responce = requests.get(url, params=self.params_hits, headers=headers)
        elif source == 'visits':
            responce = requests.get(url, params=self.params_visits, headers=headers)

        print(responce)
        if responce.status_code == 200:
            return json.loads(responce.text)['log_request']['status'], responce
        else:
            ValueError(responce.text)

    def download(self, request_id, parts, type):
        headers = {'Authorization': 'OAuth ' + self.__TOKEN}
        all_dfs = []
        for part in parts:
            part_num = part['part_number']
            url = '{host}/management/v1/counter/{counter_id}/logrequest/{request_id}/part/{part}/download' \
                .format(
                host=self.API_HOST,
                counter_id=self.__COUNTER_ID,
                request_id=request_id,
                part=part_num
            )
            response = requests.get(url, headers=headers)
            df = pd.read_csv(StringIO(response.text), sep='\t')
            all_dfs.append(df)
            res = pd.concat(all_dfs)
            if type == 'visits':
                res['ym:s:visitID'] = res['ym:s:visitID'].astype(str)
                res['ym:s:clientID'] = res['ym:s:clientID'].astype(str)
            elif type == 'hits':
                res['ym:pv:watchID'] = res['ym:pv:watchID'].astype(str)
                res['ym:pv:clientID'] = res['ym:pv:clientID'].astype(str)
            res.columns = res.columns.str.replace(":", "_")
            res.columns = map(str.lower, res.columns)
        return res