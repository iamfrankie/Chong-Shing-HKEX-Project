from annual_report import AnnualReport
from hkex_api import HKEX_API
import database as DB
from logger import Logger
import re, os
from fuzzywuzzy import process, fuzz


class Worker(Logger):

    def __init__(self, query=HKEX_API(), verbose=True):
        super().__init__()
        self.db = DB.DataBase()
        self.query = query
        if verbose:
            super().show_stream_log()

    @property
    def last_entry(self, table=DB.AnnualReport):
        last_entry = self.db.last_entry(table)
        self.logger.info(f'Last entry of {table.__tablename__}: {last_entry}')
        return last_entry

    @property
    def all_news_ids(self, table=DB.AnnualReport):
        news_ids = self.db.all_news_ids(table)
        return news_ids

    @property
    def query(self):
        return self._query

    @query.setter
    def query(self, query):
        self._query = query
        self._datas = self._query.get_data()
        all_news_ids = self.all_news_ids
        self._datas = [data for data in self._datas if int(
            data.news_id) not in all_news_ids]
        self.logger.info(
            f'{len(self._datas)} rows of new data since last run.')

    @property
    def datas(self):
        return self._datas

    @Logger.track
    def start(self):
        datas = self.datas
        for data in datas:
            self.logger.info(f'Start processing {data}')

            annual_report = AnnualReport.create(
                news_id=data.news_id,
                date_time=data.date_time,
                stock_code=data.stock_code,
                stock_name=data.stock_name,
                title=data.title,
                long_text=data.long_text,
                src=data.file_link,
                file_info=data.file_info
            )

            if annual_report:
                try:
                    annual_report.add_to_db()
                except Exception as e:
                    self.logger.error(e)
                    continue
            self.logger.info(f'Finish processing {data}')
            self.logger.info(
                f'{datas.index(data)/len(datas):.2%} complete. {len(datas) - (datas.index(data) + 1)} remains.')

    def get_market_share(self, pct=False, alphabetical_order=False):
        df_auditors = self.db.query_auditors()
        df_auditors['auditors'] = df_auditors.auditors.apply(lambda auditor: self.validate_auditor(auditor, others = 'Others'))
        df_market_share = (df_auditors.auditors.value_counts(normalize=True).mul(100).round(2).astype(str) + '%').rename_axis('auditors').to_frame('pct') if pct else \
            df_auditors.auditors.value_counts().rename_axis('auditors').to_frame('count')
        return df_market_share.sort_index() if alphabetical_order else df_market_share

    def kam_tags_to_csv(self, dir_='kam_tags'):
        df_kam_tags = self.db.query_kams_tags()
        kam_tags = df_kam_tags.tag.sort_values().unique().tolist()
        cols = ['id', 'news_id', 'date_time', 'stock_code', 'stock_name', 'title',
                'long_text', 'file_info', 'audit_firms', 'kams', 'kam_tags', 'file_link']
        if not os.path.exists(dir_):
                os.makedirs(dir_)
        for tag in kam_tags:
            df_relative_annual_report = self.db.query_annual_report_with_kam_tags(
                tag)
            df_relative_annual_report = df_relative_annual_report[cols]
            csv_path = f'./{dir_}/{tag.replace(" ", "_")}.csv'
            df_relative_annual_report.to_csv(csv_path, index=False)

    def auditors_to_csv(self, dir_='auditors'):
        df_auditors = self.db.query_auditors()
        auditors = df_auditors.auditors.sort_values().unique().tolist()
        cols = ['id', 'news_id', 'date_time', 'stock_code', 'stock_name', 'title',
                'long_text', 'file_info', 'audit_firms', 'kams', 'kam_tags', 'file_link']
        if not os.path.exists(dir_):
                os.makedirs(dir_)
        for auditor in auditors:
            df_relative_annual_report = self.db.query_annual_report_with_auditors(auditor, case_insensitive=False)
            df_relative_annual_report = df_relative_annual_report[cols]
            auditor = self.validate_auditor(auditor, others='Others')
            csv_path = f'./{dir_}/{auditor.replace(" ", "_")}.csv'
            
            if not os.path.isfile(csv_path):
                df_relative_annual_report.to_csv(csv_path, index=False)
            else:
                df_relative_annual_report.to_csv(csv_path, mode='a', header=False, index=False)

    def validate_auditor(self, r_auditor, min_similarity=90, others = None):
        v_auditors = self.db.query_valid_auditors().v_auditors.to_list()
        _r_auditor = re.sub(r's*(limited|Touche Tohmatsu)','', r_auditor, flags=re.IGNORECASE)
        valid_auditor = process.extractOne(
            _r_auditor, v_auditors, scorer=fuzz.token_set_ratio)[0]
        similarity = process.extractOne(
            _r_auditor, v_auditors, scorer=fuzz.token_set_ratio)[1]
        if len(_r_auditor) <= 4:
            min_similarity = 80
        if others:
            return valid_auditor.upper() if similarity >= min_similarity else others.upper()
        return valid_auditor.upper() if similarity >= min_similarity else _r_auditor
    



if __name__ == '__main__':
    worker = Worker()
    # df_auditors = worker.db.query_auditors()
    # df_auditors = df_auditors.auditors.apply(lambda auditor: worker.validate_auditor(auditor))
    # print(df_auditors)
    # print(worker.get_market_share(pct=True))
    worker.kam_tags_to_csv()
    worker.auditors_to_csv()
