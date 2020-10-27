from pdf import PDF, Outline, ReportOutline, PageWithSection
from helper import flatten
import datetime
import re
import pandas as pd


class CorporateGovReport(ReportOutline):

    title_regex = r'^(?=.*report).*corporate governance.*$'

    def __init__(self, outline):
        super().__init__(outline)

    @property
    def audit_fee(self):
        return AuditFee.retrieve(self.pages)


class AuditFee(PageWithSection):

    section_regex = r"^(?!.*Nomination|.*Report)(?=.*REMUNERATION|.*independent|.*external|.*Accountability).*audit.*$"

    def __init__(self, pages):
        super().__init__(pages)

    @property
    def sections(self):
        return flatten([page.get_section(AuditFee.section_regex) for page in self.pages])

    @property
    def tables(self):
        return [AuditFeeTable.retrieve(section) for section in self.sections]


class AuditFeeTable:
    setting = {
        "vertical_strategy": "text",
        "horizontal_strategy": "text"
    }
    code = ['HKD', 'USD', 'RMB', 'CNY', 'RM']
    symbol_native = ['HK\$', 'US\$', 'S\$']
    symbol =['\$']
    curr = fr'({"|".join(code + symbol_native + symbol)})'

    currency_regex = curr + r'[’ ]*' + r'(0{3})?' # '|'.join([code, symbol_native, symbol])
    financial_num_regex = r'^\d{1,3}(?:([.,])(?:\d{3}\1)*\d{3})?(?:[.,]\d*)?$'
    year_regex = '|'.join(map(str, range(int(datetime.datetime.now().year) - 2, int(datetime.datetime.now().year) + 1)))

    def __init__(self, section):
        self.section = section

    @classmethod
    def retrieve(cls, section):
        return cls(section.page)

    @classmethod
    def set_year_regex(cls, current_year: int):
        cls.year_regex = '|'.join(
            map(str, range(current_year - 2, current_year + 1)))
        print(f'Set {cls.__name__}.year_regex to {cls.year_regex}')

    @property
    def raw_table(self):
        return self.section.extract_table(AuditFeeTable.setting)

    @property
    def df_raw_table(self):
        df = pd.DataFrame(self.raw_table)
        return df

    @staticmethod
    def currency_element(x):
        return x.str.fullmatch(AuditFeeTable.currency_regex)

    @staticmethod
    def financial_num_element(x):
        return x.str.fullmatch(AuditFeeTable.financial_num_regex)

    @staticmethod
    def year_element(x):
        return x.str.fullmatch(AuditFeeTable.year_regex)

    @staticmethod
    def eng_element(x):
        return x.str.contains(r'[A-Za-z0-9]+')

    @staticmethod
    def get_cond_cols(df, element_cond_func):
        return df.apply(element_cond_func).any()

    @staticmethod
    def get_cond_rows(df, element_cond_func):
        return df.apply(element_cond_func).any(axis=1)

    @property
    def df_fee(self):
        df = self.df_raw_table
        currency_cols = self.get_cond_cols(df, self.currency_element)
        currency_rows = self.get_cond_rows(df, self.currency_element)
        financial_num_rows = self.get_cond_rows(df, self.financial_num_element)
        year_rows = self.get_cond_rows(df, self.year_element)
        df_fee = df.loc[(financial_num_rows | year_rows| currency_rows), currency_cols]
        return df_fee

    @property
    def df_filtered_table(self):
        df_fee = self.df_fee
        df_filtered_table = self.df_raw_table.iloc[df_fee.index]
        df_filtered_table = df_filtered_table.loc[:, self.get_cond_cols(df_filtered_table, self.eng_element)]
        return df_filtered_table

    @property
    def df_clean_table(self):
        df_filtered_table = self.df_filtered_table

        def df_header(element_cond_func):
            financial_num_cols = self.get_cond_cols(
                df_filtered_table, self.financial_num_element)
            rows = self.get_cond_rows(df_filtered_table, element_cond_func)
            row_idx = df_filtered_table.loc[rows, financial_num_cols].index
            row = df_filtered_table.loc[row_idx]
            header = row[row.apply(element_cond_func, axis=1)].fillna('')
            return header, row_idx

        year_header, year_row_idx = df_header(self.year_element)
        currency_header, currency_row_idx = df_header(self.currency_element)

        if not year_header.empty and not currency_header.enpty:
            df_filtered_table.columns = pd.MultiIndex.from_arrays(
                [year_header.iloc[0], currency_header.iloc[0]], names=['year', 'currency'])
        else:
            df_filtered_table.columns = pd.MultiIndex.from_arrays(
                [currency_header.iloc[0]], names=['currency'])
        df_clean_table = df_filtered_table.drop(
            year_row_idx.append(currency_row_idx))
        df_clean_table.reset_index(drop=True, inplace=True)
        return df_clean_table

    @property
    def currency(self):
        pass

    @property
    def currency_unit(self):
        pass

    @property
    def total(self):
        pass

    def __repr__(self):
        return f'{self.__class__.__name__} - {self.section}'


if __name__ == '__main__':
    from hkex_api import HKEX_API
    # https://www1.hkexnews.hk/listedco/listconews/gem/2020/0929/2020092901098.pdf #concat number
    # https://www1.hkexnews.hk/listedco/listconews/sehk/2020/0929/2020092900604.pdf #concat number
    query = HKEX_API()
    urls = [data.file_link for data in query.get_data()]
    # urls = ['https://www1.hkexnews.hk/listedco/listconews/sehk/2020/0923/2020092300374.pdf']
    for url in urls:
        # url = data.file_link
        # url, p = 'https://www1.hkexnews.hk/listedco/listconews/sehk/2020/0721/2020072100713.pdf', 61
        # url, p = 'https://www1.hkexnews.hk/listedco/listconews/sehk/2020/0721/2020072100653.pdf', 94
        print(url)
        pdf = PDF.create(url)
        corp_gov_report = pdf.get_outline(CorporateGovReport.title_regex)
        if not corp_gov_report:
            continue
        corp_gov_report = CorporateGovReport.create(corp_gov_report[0])
        if not corp_gov_report:
            continue
        if not corp_gov_report.audit_fee:
            continue
        try:
            page = corp_gov_report.audit_fee.pages[0]
            sec = corp_gov_report.audit_fee.sections[0]
            table = corp_gov_report.audit_fee.tables[0]
        except Exception as e:
            print(e)
            continue
        try:
            print(table.df_clean_table)
        except IndexError:
            print(table.df_raw_table)
    # print(sec.text)
    # print(corp_gov_report.pages)

    # page = pdf.
