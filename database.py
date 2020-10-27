from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy import create_engine, Column, ForeignKey, inspect, desc, func
from sqlalchemy.types import Integer, String, DateTime, Text, LargeBinary
from sqlalchemy.ext.hybrid import hybrid_property, hybrid_method
from datetime import datetime
from contextlib import contextmanager
from helper import flatten
import pandas as pd
import random
from typing import Union
# from logger import Logger

name = 'foo2'
path = f'sqlite:///{name}.db'
engine = create_engine(path, echo=True)
Base = declarative_base(bind=engine)


class AnnualReport(Base):
    __tablename__ = 'annual_report'
    id = Column(Integer, primary_key=True)
    news_id = Column(Integer, nullable=False, unique=True)
    date_time = Column(String, nullable=False)
    stock_code = Column(Integer, nullable=False)
    stock_name = Column(String, nullable=False)
    title = Column(String, nullable=False)
    long_text = Column(String, nullable=False)
    file_info = Column(String, nullable=False)
    file_link = Column(String, nullable=False)
    # bytesIO_obj = Column(LargeBinary, nullable=False)
    created_on = Column(DateTime, default=datetime.now)
    updated_on = Column(DateTime, default=datetime.now, onupdate=datetime.now)
    audit_firms = relationship(
        'Auditor', backref='annual_report')  # one to many
    kams = relationship(
        'KeyAuditMatter', backref='annual_report')  # one to many
    kam_tags = relationship('KeyAuditMatterTag',
                            backref='annual_report')  # one to many

    def __repr__(self):
        return f"<{self.__class__.__name__}({self.news_id}, {self.date_time}, {self.stock_code}, {self.file_link})>"


class Auditor(Base):
    __tablename__ = 'auditor'
    id = Column(Integer, primary_key=True)
    news_id = Column(Integer, ForeignKey('annual_report.news_id'))
    name = Column(Text, nullable=False)

    def __repr__(self):
        return f"<{self.__class__.__name__}({self.id}, {self.news_id}, {self.name})>"


class KeyAuditMatter(Base):
    __tablename__ = 'key_audit_matter'
    id = Column(Integer, primary_key=True)
    news_id = Column(Integer, ForeignKey('annual_report.news_id'))
    item = Column(Text)
    tags = relationship('KeyAuditMatterTag', backref='kam_item')

    def __repr__(self):
        return f"<{self.__class__.__name__}({self.id}, {self.news_id} {self.item})>"


class KeyAuditMatterTag(Base):
    __tablename__ = 'key_audit_matter_tag'
    id = Column(Integer, primary_key=True)
    news_id = Column(Integer, ForeignKey('annual_report.news_id'))
    kam_id = Column(Integer, ForeignKey('key_audit_matter.id'))
    tag = Column(String)

    def __repr__(self):
        return f'<{self.__class__.__name__}({self.id}, {self.kam_id}, {self.tag})>'


class KeyAuditMatterKeywords(Base):
    __tablename__ = 'key_audit_matter_keywords'
    id = Column(Integer, primary_key=True)
    keyword = Column(String, nullable=False, unique=True)

    def __repr__(self):
        return f'<{self.__class__.__name__}({self.id}, {self.keyword})>'


class ValidatedAuditor(Base):
    __tablename__ = 'validated_auditor'
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)

    def __repr__(self):
        return f'<{self.__class__.__name__}({self.id}, {self.name})>'


class CommonCurrency(Base):
    __tablename__ = 'common_currency'
    id = Column(Integer, primary_key=True)
    code = Column(String, nullable=False, unique=True)
    symbol = Column(String)
    symbol_native = Column(String)

    def __repr__(self):
        return f'<{self.__class__.__name__}({self.id}, {self.code}, {self.symbol}, {self.symbol_native})>'


class DataBase:
    INIT_KAM_KEYWORDS_CSV = 'kam_keywords.csv'
    INIT_CURRENCY_JSON = 'Common-Currency.json'
    INIT_VALID_AUDITORS = 'valid_auditors.csv'

    def __init__(self, path=path):
        self.path = path

    @classmethod
    def init(cls, Base=Base, path=path):
        engine = create_engine(path, echo=True)
        Base.metadata.create_all(engine)
        cls.init_kam_keywords(engine)
        cls.init_currencies(engine)
        cls.init_valid_auditors(engine)
        return cls(path=path)

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, path):
        self._path = path
        self._engine = create_engine(path, echo=True)
        self._Session = sessionmaker(bind=self._engine)

    @property
    def engine(self):
        return self._engine

    @property
    def inspector(self):
        return inspect(self.engine)

    @contextmanager
    def Session(self):
        session = self._Session()
        try:
            yield session
            session.commit()
        except:
            session.rollback()
            print('SESSION ROLLBACK!!')
            raise
        finally:
            session.close()
    
    @classmethod
    def init_valid_auditors(cls, engine):
        df_v_auditors = pd.read_csv(cls.INIT_VALID_AUDITORS, names=['name']).drop_duplicates('name')
        try:
            df_v_auditors.to_sql('validated_auditor', con=engine,
                           if_exists='append', index=True, index_label='id')
        except Exception as e:
            print(e)



    @classmethod
    def init_kam_keywords(cls, engine):
        kam_kws = pd.read_csv(cls.INIT_KAM_KEYWORDS_CSV, names=['keyword'])
        try:
            kam_kws.to_sql('key_audit_matter_keywords', con=engine,
                           if_exists='append', index=True, index_label='id')
        except Exception as e:
            print(e)

    @classmethod
    def init_currencies(cls, engine):
        currency = pd.read_json(cls.INIT_CURRENCY_JSON)
        currency = currency.T.reset_index(drop=True)
        cols = ['code', 'symbol', 'symbol_native']
        try:
            currency[cols].to_sql(
                'common_currency', con=engine, if_exists='append', index=True, index_label='id')
        except Exception as e:
            print(e)

    @property
    def tables(self) -> list:
        inspector = self.inspector
        tables = {tablename: [column['name'] + '*' if column['primary_key'] else column['name'] + '+' if column['name'] in flatten(
            [fk_col['constrained_columns'] for fk_col in inspector.get_foreign_keys(tablename)]) else column['name'] for column in inspector.get_columns(tablename)] for tablename in inspector.get_table_names()}
        return tables

    def show_tables(self):
        return {tablename: pd.DataFrame(pd.read_sql(f"select * from {tablename}", con=self.engine)) for tablename in self.tables.keys()}

    def last_entry(self, table=AnnualReport):
        with self.Session()as session:
            last_entry = session.query(table).order_by(desc(table.id)).limit(1)
        return last_entry.first()
    
    def all_news_ids(self, table = AnnualReport):
        with self.Session() as session:
            q = session.query(table).filter(table.news_id)
        return [i.news_id for i in q.all()]


    def add(self, instance):
        with self.Session() as session:
            session.add(instance)


    def add_all(self, iterables_instance):
        with self.Session() as session:
            session.add_all(iterables_instance)
    
    def query_auditors(self):
        with self.Session() as session:
            query = session.query(Auditor.name)
        results = query.all()
        return pd.DataFrame(results, columns = ['auditors'])

    def query_valid_auditors(self):
        with self.Session() as session:
            query = session.query(ValidatedAuditor.name)
        results = query.all()
        return pd.DataFrame(results, columns = ['v_auditors'])
    
    def query_kams_tags(self):
        with self.Session() as session:
            query = session.query(KeyAuditMatterTag.tag).distinct()
        results = query.all()
        return pd.DataFrame(results, columns = ['tag'])
    

    def query_annual_report_with_auditors(self, auditors:Union[list, str] = [], case_insensitive=True):
        auditors = auditors if type(auditors) is list else [auditors]
        with self.Session() as session:
            if case_insensitive:
                auditors = [auditor.lower for auditor in auditors]
                query = session.query(AnnualReport).join(Auditor).filter(func.lower(Auditor.name).in_(auditors)) if auditors else session.query(AnnualReport).join(Auditor)
            else:
                query = session.query(AnnualReport).join(Auditor).filter(Auditor.name.in_(auditors)) if auditors else session.query(AnnualReport).join(Auditor)
        results = query.all()
        print(f'{len(results)} annual report is produced by {auditors}')
        df = self.query_to_df(results)
        return df
    

    def query_annual_report_with_kam_tags(self, kam_tags: Union[list, str] = []):
        kam_tags = kam_tags if type(kam_tags) is list else [kam_tags]
        with self.Session() as session:
            query = session.query(AnnualReport).join(KeyAuditMatterTag).filter(KeyAuditMatterTag.tag.in_(kam_tags)) if kam_tags else session.query(AnnualReport).join(KeyAuditMatterTag)
        results = query.all()
        print(f'{len(results)} annual report is produced by {kam_tags}')
        return self.query_to_df(results)
    

    def query_annual_report_with_auditors_and_kam_tags(self, auditors:Union[list, str, None] = [], kam_tags:Union[list, str, None] = []):
        df_annual_report_auditors = self.query_annual_report_with_auditors(auditors)
        df_annual_report_kam_tags = self.query_annual_report_with_kam_tags(kam_tags)
        exclusive_cols =['audit_firms', 'kams', 'kam_tags', 'id', 'created_on', 'updated_on']
        merge_keys = [col for col in df_annual_report_auditors.columns if col not in exclusive_cols]
        df = pd.merge(df_annual_report_auditors, df_annual_report_kam_tags, on = merge_keys)
        trail_cols = sorted([col for col in df.columns if col.endswith('_x') or col.endswith('_y')], key = lambda string: string[-1])
        cols = [col for col in df.columns if col not in trail_cols] + trail_cols
        df = df[cols]
        df['date_time'] = pd.to_datetime(df.date_time)
        return df
    


    @staticmethod
    def query_to_df(rset):
        """
        List of result
        Return: columns name, list of result
        """
        result = []
        for obj in rset:
            instance = inspect(obj)
            items = instance.attrs.items()
            result.append([x.value for _ , x in items])
        df = pd.DataFrame(result, columns = instance.attrs.keys())
        return df
    


# DataBase.init()
if __name__ == "__main__":
    db = DataBase()
    # big4 = ['Deloitte Touche Tohmatsu', 'PricewaterhouseCoopers', 'Ernst & Young', 'KPMG']
    # tags = ['goodwill', 'income']
    # df = db.query_annual_report_with_auditors_and_kam_tags(big4, tags)
    # df_kams_tags = db.query_kams_tags()
    