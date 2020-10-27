from bs4 import BeautifulSoup as bs
from typing import Union
import requests, re, pandas as pd

class WikiPage:
    def __init__(self, url):
        self.url = url
    
    @property
    def url(self):
        return self._url
    
    @url.setter
    def url(self, url):
        if not self.is_url(url):
            raise ValueError(f'{url} is not url')
        with requests.get(url) as response:
            response.raise_for_status()
            self._soup = bs(response.content, 'html.parser')
        self._url = url
    
    
    @property
    def soup(self):
        return self._soup
    
    @property
    def tables(self):
        tables = self.soup.find_all('table', {'class' : 'wikitable'})
        table_names = [re.sub(r'\n|\[.*?\]','',table.caption.text) for table in tables]
        print(table_names)
        # return {table_name: self.html_table_to_df(table) for table_name, table in zip(table_names, tables)}
    
    @property
    def table_names(self):
        pass
        # see https://stackoverflow.com/questions/27681602/find-previous-occurrence-of-an-element

    @staticmethod
    def html_table_to_df(table):
        data = []
        tab_cols = [re.sub(r'\n|\[.*?\]','',col.text) for col in table.find_all('th')]
        for row in table.find_all('tr'):
            tab_row = [re.sub(r'\n|\[.*?\]','',cell.text) for cell in row.find_all('td')]
            tab_row = dict(zip(tab_cols, tab_row))
            data.append(tab_row)
        df = pd.DataFrame(data)
        return df.dropna()

    @staticmethod
    def is_url(src: str) -> bool:
        if not isinstance(src, str):
            logging.warning(f'Input type {type(src)} is not str.')
            return None
        url_regex = re.compile(
            r'^(?:http|ftp)s?://'  # http:// or https://
            # domain...
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|'
            r'localhost|'  # localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        return True if re.match(url_regex, src) else False
    
    
    def find_tables(self, regex):
        return {table_name: table for table_name, table in self.tables.items() if re.search(regex, table_name, re.I)}
    
    def __repr__(self):
        return f'<{self.__class__.__name__} {self.url}>'

class CurrencyCode(WikiPage):
    wiki_url = 'https://en.wikipedia.org/wiki/ISO_4217'
    official_code_title = r'Active ISO 4217 currency codes'
    unofficial_code_title = r'Active non-ISO 4217 currency codes'
    
    def __init__(self):
        super().__init__(self.wiki_url)
    
    @property
    def common_code_table(self) -> pd.DataFrame:
        official_code_table = self.official_code_table
        unofficial_code_table = self.unofficial_code_table
        common_col = {u: o for u, o in zip(unofficial_code_table.columns, official_code_table.columns)}
        common_code_table = official_code_table.append(unofficial_code_table.rename(columns = common_col))
        return common_code_table.sort_values(by = ['Code'])
    
    @property
    def common_currency_code(self) -> list():
        return self.common_code_table.Code.to_list()

    @property
    def official_code_table(self) -> Union[pd.DataFrame, None]:
        return self.tables.get(self.official_code_title, None)
    
    @property
    def unofficial_code_table(self) -> Union[pd.DataFrame, None]:
        return self.tables.get(self.unofficial_code_title, None)

class CurrencySymbol(WikiPage):
    wiki_url  = 'https://en.wikipedia.org/wiki/Currency_symbol'

    def __init__(self):
        super().__init__(self.wiki_url)
        


    

if __name__ == "__main__":
    cs = CurrencySymbol()
    print(cs.tables)
    

    # cc = CurrencyCode(currency_code_url)
    # cc = CurrencyCode()
    # print(cc.common_currency_code)
    # official_code_col = cc.official_code_table.columns
    # unofficial_code_col = cc.unofficial_code_table.columns
    # print({o: u for o, u in zip(official_code_col, unofficial_code_col)})

    # print(cc.tables)

    # cc = WikiPage(currency_code_url)
    # for n, tab in cc.tables.items():
    #     print(n)
    #     print(tab) 