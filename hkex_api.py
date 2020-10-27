import json, requests, datetime, html, logging
from typing import Generator
from collections import namedtuple
from logger import Logger

class HKEX_API(Logger):
    
    '''
    usage:
        make query to hkex api with specified time range stock id and document type
    
    note:
        - query date format is '%Y%m%d' e.g. '20200825' (yyyymmdd)
        - stock id is complusory for query which is over a year
        - document types are annual report, half year report and quaterly report. (default: annual report)
    '''
    
    endpoint = 'https://www1.hkexnews.hk/search/titleSearchServlet.do'
    dt_fmt = '%Y%m%d'
    doc_code = {
        'annual_report': '40100',
        'half_year_report': '40200',
        'quaterly_report': '40300'    
    }

    
    def __init__(self, from_date=None, to_date=None, stock_id=None, doc='annual_report'):
        super().__init__()
        self.from_date = from_date
        self.to_date = to_date
        self.stock_id = stock_id
        self.doc = doc
    
    @property
    def from_date(self) -> str:
        return self._from_date
    
    
    @from_date.setter
    def from_date(self, from_date:str) -> str:
        from_date = from_date or HKEX_API.n_yearsago(n=1)
        self._from_date = self.date_fmt_validator(from_date)
        self.logger.info(f'{self}.from_date set to {self._from_date}')
    
    
    @property
    def to_date(self) -> str:
        return self._to_date
    
    @to_date.setter
    def to_date(self, to_date:str) -> str:
        to_date = to_date or HKEX_API.today()
        self._to_date = self.date_fmt_validator(to_date)
        self.logger.info(f'{self}.to_date set to {self._to_date}')
    
    @property
    def doc(self) -> str:
        return self._doc
    
    @doc.setter
    def doc(self, doc) -> str:
        self._doc = doc
        self.logger.info(f'{self}.doc set to {self._doc}')

    @property
    def payloads(self) -> dict:
        
        from_date=self.from_date
        to_date=self.to_date
        stock_id=self.stock_id
        doc=self.doc

        if HKEX_API.over_a_year(from_date=from_date) and stock_id is None:
            ytd = HKEX_API.n_yearsago(1)
            self.logger.error(f'Query over a year must specify stock_id e.g "1", global query can only from "{ytd}"')
            raise ValueError(f'Query over a year must specify stock_id e.g "1", global query can only from "{ytd}"')

        payloads = {
            'sortDir': '0',
            'sortByOptions': 'DateTime',
            'category': '0',
            'market': 'SEHK',
            'stockId': stock_id or '-1',
            'documentType': '-1',
            'fromDate': from_date,
            'toDate': to_date,
            'title': '',
            'searchType': '1',
            't1code': '40000',
            't2Gcode': '-2', 
            't2code': HKEX_API.doc_code.get(doc, None) or '40100', # 40100: annual report, 40200: half_year_report, 40300: quaterly_report
            'rowRange': '100',
            'lang': 'EN'
        }
        
        return payloads


    @staticmethod
    def data_decoder(data):
        data = {k.lower(): html.unescape(v) for k, v in data.items()}
        data['file_link'] = "https://www1.hkexnews.hk" + data['file_link']
        return namedtuple('data', data.keys())(*data.values())
    
    
    @staticmethod
    def date_fmt_validator(date_str:str) -> None:
        try:
            datetime.datetime.strptime(date_str, HKEX_API.dt_fmt)
            return date_str
        except ValueError as e:
            self.logger.error(f'{e}, {date_str} format is not {HKEX_API.dt_fmt}')
            raise ValueError(f'{e}, {date_str} format is not {HKEX_API.dt_fmt}')
    

    @staticmethod
    def call_api(endpoint:str, payloads:dict) -> tuple:
               
        with requests.get(endpoint, params=payloads) as response:
            response.raise_for_status()
            site_json = json.loads(response.text)
            if site_json['hasNextRow']:
                payloads['rowRange'] = site_json['recordCnt']
                return HKEX_API.call_api(endpoint, payloads=payloads)
            results = json.loads(site_json['result'], object_hook = HKEX_API.data_decoder)
            return tuple(results)
    

    @staticmethod
    def yesterday() -> str:
        '''
        return yesterday string value in HKEX_API.dt_fmt format
        '''
        dt_yesterday = datetime.date.today() - datetime.timedelta(days=1)
        fmt_yesterday = dt_yesterday.strftime(HKEX_API.dt_fmt)
        return fmt_yesterday

    @staticmethod
    def today() -> str:
        '''
        return today string value in HKEX_API.dt_fmt format
        '''
        dt_today = datetime.date.today()
        fmt_today = dt_today.strftime(HKEX_API.dt_fmt)
        return fmt_today

    @staticmethod
    def over_a_year(from_date, to_date=datetime.date.today()) -> bool:
        '''
        return if from_date to to_date is over a year 
        '''
        ytd = HKEX_API.n_yearsago(1, to_date)
        datetime_ytd = datetime.datetime.strptime(ytd, HKEX_API.dt_fmt)
        datetime_frm_date = datetime.datetime.strptime(from_date, HKEX_API.dt_fmt)
        return datetime_frm_date < datetime_ytd

    @staticmethod
    def n_yearsago(n:int, from_date=datetime.datetime.now()) -> str:
        try:
            dt_yearsago = from_date.replace(year=from_date.year - n)
        except ValueError:
            # Must be 2/29!
            assert from_date.month == 2 and from_date.day == 29  # can be removed
            dt_yearsago = from_date.replace(month=2, day=28, year=from_date.year - n)
        finally:
            return dt_yearsago.strftime(HKEX_API.dt_fmt)
    
    @Logger.track
    def get_data(self) -> list:
        data = HKEX_API.call_api(endpoint=HKEX_API.endpoint, payloads=self.payloads)
        self.logger.info(f'{len(data)} row of data from {self.from_date} to {self.to_date}')
        return data
    
    def __repr__(self):
        return f'{self.__class__.__name__}'
if __name__ == '__main__':
    # pass
    # print(_get_data())
    # query = HKEX_API(from_date=yesterday(), to_date=today())
    # query = HKEX_API(from_date=n_yearsago(n=1), to_date=today())
    # query = HKEX_API(from_date=HKEX_API.n_yearsago(n=1), to_date=HKEX_API.today())
    query = HKEX_API()
    print(query.doc)
    print(query.payloads['t2code'])
    print(len(query.get_data()))
    print(len([i for i in query.get_data()]))
    # query = HKEX_API(from_date=n_yearsago(n=1), to_date=today(), doc='half_year_report') 
    print('>>>>>>')
    query.doc = 'half_year_report'
    print(query.doc)
    print(query.payloads['t2code'])
    print(len(query.get_data()))
    print(len([i for i in query.get_data()]))
    print('>>>>>>')
    print(query.from_date)
    query.from_date = HKEX_API.yesterday()
    print(query.from_date)
    print(query.payloads['fromDate'])
    print(len(query.get_data()))
    print(len([i for i in query.get_data()]))
    # print(query.data)
    # query.data = 'hehe'