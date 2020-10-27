from audit_report import IndependentAuditorReport, KeyAuditMatter
from pdf import PDF
from hkex_api import HKEX_API
import pandas as pd, re
from helper import flatten
import database as DB
from logger import Logger
class AnnualReport(PDF, Logger):
    def __init__(self, src, news_id, date_time, stock_code, stock_name, title, long_text, file_info):
        super().__init__(src)
        self.news_id = news_id
        self.date_time = date_time
        self.stock_code = stock_code
        self.stock_name = stock_name
        self.title = title
        self.long_text = long_text
        self.src = src
        self.file_info = file_info
    
    @classmethod
    def create(cls, **kwargs):
        try:
            return cls(**kwargs)
        except Exception as e:
            cls.get_module_logger().error(e)
            return None
    
    @PDF.src.setter
    def src(self, src):
        PDF.src.fset(self, src)
        self.logger.info(f'Processing indepdentent audit reports')
        audit_report_outlines = self.get_outline(IndependentAuditorReport.title_regex)
        audit_reports = [IndependentAuditorReport.create(outline) for outline in audit_report_outlines if audit_report_outlines]
        self._audit_reports = audit_reports
        self.logger.info(f'Indepdentent audit reports: {self._audit_reports} ready')
        
            
    @property
    def audit_reports(self) -> list:
        return self._audit_reports

    @property
    def auditors(self) -> set:
        audit_reports = self.audit_reports
        auditors = [list(audit_report.auditors) for audit_report in audit_reports if audit_reports and audit_report.auditors]
        auditors = {auditor for auditor in flatten(auditors) if auditor}
        return auditors
    
    @property
    def kams(self):
        audit_reports = self.audit_reports
        kams = [audit_report.kams for audit_report in audit_reports if audit_reports and audit_report.kams]
        return kams
    
    @Logger.track
    def add_annual_report_to_db(self, session):
        annual_report_record = DB.AnnualReport(
                news_id = self.news_id, 
                date_time = self.date_time, 
                stock_code = self.stock_code, 
                stock_name = self.stock_name, 
                title = self.title, 
                long_text = self.long_text, 
                file_info = self.file_info,
                file_link = self.src
                )
        
        session.add(annual_report_record)
        session.commit()
        self.logger.info(f'>> annual report: {annual_report_record} inserted to {annual_report_record.__tablename__}')
    
    @Logger.track
    def add_auditors_to_db(self, session):
        auditors = self.auditors
        for auditor in auditors:
            auditor_record = DB.Auditor(news_id = self.news_id, name = auditor)
            session.add(auditor_record)
            session.commit()
            self.logger.info(f'>> auditor: {auditor_record} inserted to {auditor_record.__tablename__}')
            
    @Logger.track
    def add_kams_and_kam_tags_to_db(self, session):
        kam_items = flatten([kam.items for kam in self.kams])
        for kam_item in kam_items:
            kam_item_record = DB.KeyAuditMatter(news_id = self.news_id, item = kam_item)
            session.add(kam_item_record)
            session.commit()
            self.logger.info(f'>> kam_item: {kam_item_record} inserted to {kam_item_record.__tablename__}')
            self.add_kam_tags_to_db(session = session, kam_item_record=kam_item_record)
    
    @Logger.track
    def add_kam_tags_to_db(self, session , kam_item_record):
        tags = KeyAuditMatter.get_tags(kam_item_record.item)
        for tag in tags:
            kam_tag_record = DB.KeyAuditMatterTag(news_id = self.news_id, kam_id = kam_item_record.id, tag = tag)
            session.add(kam_tag_record)
            session.commit()
            self.logger.info(f'>> kam_tag: {kam_tag_record} inserted to {kam_tag_record.__tablename__}')
    
    @Logger.track
    def add_to_db(self):
               
        db = DB.DataBase(DB.path)
        
        with db.Session() as session:
            self.add_annual_report_to_db(session)
            self.add_auditors_to_db(session)
            self.add_kams_and_kam_tags_to_db(session)
            # self.logger.info('Loading annual report to db')
            # annual_report_record = DB.AnnualReport(
            #     news_id = self.news_id, 
            #     date_time = self.date_time, 
            #     stock_code = self.stock_code, 
            #     stock_name = self.stock_name, 
            #     title = self.title, 
            #     long_text = self.long_text, 
            #     file_info = self.file_info,
            #     file_link = self.src
            #     )
        
            # session.add(annual_report_record)
            # session.commit()
            # self.logger.info(f'>> annual report: {annual_report_record} inserted to {annual_report_record.__tablename__}')
            # self.logger.info('Finish loading annual report to db')


            # auditors = self.auditors
            # self.logger.info(f'Loading auditors to db')
            # for auditor in auditors:
            #     auditor_record = DB.Auditor(news_id = annual_report_record.news_id, name = auditor)
            #     session.add(auditor_record)
            #     session.commit()
            #     self.logger.info(f'>> auditor: {auditor_record} inserted to {auditor_record.__tablename__}')
            # self.logger.info('Finish loading auditors to db')

            
            # self.logger.info('Loading kams and kam tags to db')
            # kam_items = flatten([kam.items for kam in self.kams])
            # for kam_item in kam_items:
            #     kam_item_record = DB.KeyAuditMatter(news_id = annual_report_record.news_id, item = kam_item)
            #     session.add(kam_item_record)
            #     session.commit()
            #     self.logger.info(f'>> kam_item: {kam_item_record} inserted to {kam_item_record.__tablename__}')

            #     tags = KeyAuditMatter.get_tags(kam_item)
            #     for tag in tags:
            #         kam_tag_record = DB.KeyAuditMatterTag(news_id = annual_report_record.news_id, kam_id = kam_item_record.id, tag = tag)
            #         session.add(kam_tag_record)
            #         session.commit()
            #         self.logger.info(f'>> kam_tag: {kam_tag_record} inserted to {kam_tag_record.__tablename__}')
            # self.logger.info('Finish loading kams and kam tags to db')