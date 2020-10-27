from io import BytesIO
from typing import Union, Pattern, Match
from itertools import zip_longest
from helper import flatten, utf8_str, consecutive_int_list
import pandas as pd, requests, re, logging, os, pdfplumber, PyPDF2
from logger import Logger

class PDF(Logger):
    def __init__(self, src):
        super().__init__()
        self.src = src

    @classmethod
    def create(cls, src):
        m_logger = super().get_module_logger()
        src_types = {
           type(src) is str and cls.is_url(src) : lambda: cls.byte_obj_from_url(src),
           type(src) is str and os.path.isfile(src) : lambda: open(src, 'rb'),
           type(src) is not str and cls.is_binary(src) : lambda: src
        }
        pdf_obj = src_types.get(True, lambda: None)()
        if pdf_obj and pdf_obj.read().startswith(b'%PDF'):
            return cls(src)
        m_logger.warning(f'{src} is not a valid pdf src file.')
        return None

    @property
    def src(self):
        return self._src

    @src.setter
    def src(self, src):
        self._pdf_obj = self.byte_obj_from_url(src) or src
        self._pb_pdf = pdfplumber.open(self._pdf_obj)
        self._src = src
        self.logger.info(f'{self}.src set to {self._src}')

    @property
    def pdf_obj(self) -> Union[str, object]:
        return self._pdf_obj

    @property
    def pb_pdf(self) -> object:
        return self._pb_pdf

    @property
    def pypdf_reader(self):
        pdf_obj = self.pdf_obj
        src = self.src
        if type(pdf_obj) is str and os.path.isfile(pdf_obj):
            pdf_obj = open(src, 'rb')
        return PyPDF2.PdfFileReader(pdf_obj, strict=False)

    @property
    def max_page_num(self):
        pdf = self.pb_pdf
        return len(pdf.pages)
    
    @staticmethod
    def is_binary(obj) -> bool:
        return hasattr(obj ,'read')

    @staticmethod
    def is_url(src: str) -> bool:
        if not isinstance(src, str):
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


    @staticmethod
    def byte_obj_from_url(url: str) -> object:
        '''
        get byteIO object from url
        '''
        if not PDF.is_url(url):
            return None
        with requests.get(url) as response:
            response.raise_for_status()
            byte_obj = BytesIO(response.content)
        return byte_obj

    def get_page(self, p: int):
        pdf = self.pb_pdf
        return Page.create(pdf.pages[p])

    @property
    def outlines(self):
        pypdf_reader = self.pypdf_reader
        outlines = flatten(pypdf_reader.getOutlines())
        
        def get_page_num(outline):
            try:
                return pypdf_reader.getDestinationPageNumber(outline)
            except AttributeError:
                return None
        
        outlines = [outline for outline in outlines if get_page_num(outline)]
        titles = [outline.title for outline in outlines]
        starting_pages = [get_page_num(outline) for outline in outlines]
        ending_pages = [page_num - 1 for page_num in starting_pages[1:]]
        page_ranges = zip_longest(starting_pages, ending_pages, fillvalue=max(starting_pages, default=None))
        return [Outline(title, page_range, self.pb_pdf) for title, page_range in zip(titles, page_ranges)]

    @property
    def toc(self):
        outlines = self.outlines
        outline = [[outline.title, outline.from_page, outline.to_page]
                   for outline in outlines]
        toc = pd.DataFrame(outline, columns=['title', 'from_page', 'to_page'])
        column_types = {'title': str, 'from_page': int, 'to_page': 'Int64'}
        toc = toc.astype(column_types)
        return toc
    
    @Logger.track
    def get_outline(self, regex: Pattern) -> list:
        '''
        return a list of outline that matches with the regex pattern
        '''
        outlines = self.outlines
        result = [outline for outline in outlines if outlines and re.search(regex, outline.title, flags=re.IGNORECASE)] 
        return result if result else self.search_outline(regex)
    
    @Logger.track
    def search_outline(self, regex: Pattern, scope=None) -> list:
        pdf = self.pb_pdf
        pages = scope or pdf.pages
        
        matched_page_nums = set()

        for p in pages:
            page = Page.create(p)
            if not page or page.df_feature_text.empty:
                continue
            self.logger.debug(f'searching page {page.page_number}...')
            if any(page.df_feature_text.text.str.contains(regex, flags=re.IGNORECASE)):
                matched_page_nums.add(page.page_number)
        
        matched_page_num = max(consecutive_int_list(sorted(matched_page_nums)), key=len, default=None)
        
        if matched_page_num is None:
            return []
        
        page_range = min(matched_page_num), max(matched_page_num)
        scope = 'Local' if scope else 'Global'
        return [Outline(f'{scope} search pattern: {regex}', page_range, self.pb_pdf)]

    
    def __repr__(self):
        return f'{self.__class__.__name__}(src="{self.src}")'


class Page(Logger):
    def __init__(self, page, df_lang=None):
        self.page = page
        self.df_lang = df_lang

    @classmethod
    def create(cls, page, **kwargs):
        try:
            page = cls(page=page, **kwargs)
            page.df_main_text
            page.remove_noise()
            return page
        except Exception as e:
            print(f'{cls.__name__}({page}, {kwargs}) instance initalise failed: {e}')
            return None
    
    @property
    def df_lang(self):
        return self._df_lang
    
    @df_lang.setter
    def df_lang(self, df_lang):
        self._df_lang = df_lang

    @property
    def page_number(self) -> int:
        return self.page.page_number - 1

    @property
    def text(self) -> str:
        txt = self.page.extract_text()
        txt = txt.replace('ﬁ', 'fi')  # must clean before chinese
        txt = re.sub(r'\ufeff', ' ', txt)  # clear BOM
        return txt

    @property
    def df_char(self) -> pd.DataFrame:
        df = pd.DataFrame(self.page.chars)
        df_langs = {
            'en': df[~df['text'].str.contains(r'[^\x00-\x7F]+')],
            'cn': df[df['text'].str.contains(r'[^\x00-\x7F]+')]
        }
        df = df_langs.get(self.df_lang, df)
        normal_bbx_coord = (df.x0 > 0) & (df.top > 0) & (df.x1 > 0) & (df.bottom > 0)
        normal_x1 = df['x1'] <= self.page.width
        within_bbx = normal_bbx_coord
        df_char_within_bbox = df[within_bbx]
        return df_char_within_bbox

    @property
    def df_decarative_text(self) -> pd.DataFrame:
        df_char = self.df_char
        return df_char[df_char['upright'] == 0]

    @property
    def df_main_text(self) -> pd.DataFrame:
        df_char = self.df_char
        is_main_fontname = df_char['fontname'].isin(self.main_fontname)
        is_main_fontsize = df_char['size'].isin(self.main_fontsize)
        is_upright = df_char['upright'] == 1
        mt_df = df_char[is_main_fontname & is_main_fontsize & is_upright]
        return mt_df

    @property
    def df_feature_text(self) -> pd.DataFrame:
        self.df_lang = 'en'
        df_ft = self.df_char[~self.df_char['fontname'].isin(self.main_fontname)]
        df_feature_text = df_ft.groupby(['top', 'bottom', 'fontname', 'size']).agg(
            {'x0': 'min', 'x1': 'max', 'text': lambda x: ''.join(x)}).reset_index()
        if df_feature_text.empty:
            return df_feature_text
        df_feature_text = df_feature_text[df_feature_text.text.str.contains(r'\w+')]

        self.df_lang = None
        return df_feature_text

    @property
    def df_bold_text(self) -> pd.DataFrame:
        df_feature_text = self.df_feature_text
        if df_feature_text.empty:
            return df_feature_text
        df_bt = df_feature_text[df_feature_text['size'].isin(self.main_fontsize)]
        df_bold_text = df_bt.groupby(['top', 'bottom', 'fontname', 'size']).agg(
            {'x0': 'min', 'x1': 'max', 'text': lambda x: ''.join(x)}).reset_index()
        return df_bold_text

    @property
    def df_title_text(self) -> pd.DataFrame:
        df_feature_text = self.df_feature_text
        if df_feature_text.empty:
            return df_feature_text
        df_tt = df_feature_text[df_feature_text['size'] > self.main_fontsize.max()]
        df_title_text = df_tt.groupby(['top', 'bottom', 'fontname', 'size']).agg(
            {'x0': 'min', 'x1': 'max', 'text': lambda x: ''.join(x)}).reset_index()
        if df_title_text.empty:
            return self.df_bold_text
        return df_title_text

    @property
    def df_section_text(self) -> pd.DataFrame:
        df = self.df_title_text
        if df.empty: return df
        title_interval = df['bottom'].shift() - df['top']
        indicator = (title_interval.abs() > df['size']).cumsum()
        df_section_text = df.groupby(indicator).agg({
            'top': 'first',
            'bottom': 'last',
            'fontname': 'first',
            'size': 'first',
            'x0': 'first',
            'x1': 'first',
            'text': ''.join
        })
        df_section_text['next_top'] = df_section_text.top.shift(-1)
        df_section_text.fillna(self.page.height, inplace=True)
        return df_section_text

    
    @property
    def main_fontname(self) -> pd.Series:
        return self.df_char['fontname'].mode()

    @property
    def main_fontsize(self) -> pd.Series:
        df_char = self.df_char
        return df_char[df_char['fontname'].isin(self.main_fontname)]['size'].mode()

    @property
    def bbox_main_text(self) -> tuple:

        def bbox(lang):
            self.df_lang = lang
            df_main_text = self.df_main_text
            if df_main_text.empty:
                return None
            
            x0 = df_main_text.x0.min()
            top = df_main_text.top.min()
            x1 = df_main_text.x1.max()
            bottom = df_main_text.bottom.max()
            
            df_feature_text = self.df_feature_text
            if not df_feature_text.empty:
                top = [top, df_feature_text['top'].min()]
                x1 = [x1, df_feature_text['x1'].max()]                
                self.df_lang = None
                return x0, min(top), max(x1), bottom
            self.df_lang = None
            return x0, top, x1, bottom

        en_bbx, cn_bbx = bbox('en'), bbox('cn')

        if not (en_bbx and cn_bbx):
            return en_bbx or cn_bbx
       
        x0, top, x1, bottom = zip(en_bbx, cn_bbx)
        return min(x0), min(top), max(x1), max(bottom)

    @property
    def col_division(self) -> float:
        min_x0 = self.df_title_text.x0.min()
        max_x0 = self.df_title_text.x0.max()
        x0, top, x1, bottom = self.bbox_main_text
        if min_x0 != max_x0 and max_x0 > x0:
            self.logger.info(f'There is another colmun divided at {float(max_x0)}.')
            return max_x0
        return None

    @property
    def left_column(self) -> object:
        col_division = self.col_division
        bbox_main_text = self.bbox_main_text
        if col_division is None or bbox_main_text is None:
            return None
        x0, top, x1, bottom = self.bbox_main_text
        l_bbx = x0, top, col_division, bottom
        left_col = self.page.crop(l_bbx, relative=False)
        return Section.create(left_col, title='Left Column')

    @property
    def right_column(self) -> object:
        col_division = self.col_division
        bbox_main_text = self.bbox_main_text
        if col_division is None or bbox_main_text is None:
            return None
        x0, top, x1, bottom = self.bbox_main_text
        if col_division > x1:
            return None
        try:
            r_bbx = col_division, top, x1, bottom
            right_col = self.page.crop(r_bbx, relative=False)
        except ValueError:
            r_bbx = col_division, top, self.page.width, bottom
            right_col = self.page.crop(r_bbx, relative=False)
        return Section.create(right_col, title='Right Column')

    @property
    def sections(self) -> list:
        df_section_text = self.df_section_text
        if df_section_text.empty: return []

        def section_bbx(self, section):
            x0, top, x1, bottom = self.bbox_main_text
            x0, top, x1, bottom = x0, section.top, x1, section.next_top
            if top >= bottom:
                return None
            return x0, top, x1, bottom

        sections = []
        for sec in df_section_text.itertuples(index=False):
            sec_bbx = section_bbx(self, sec)
            if not sec_bbx: continue
            section = self.page.crop(sec_bbx, relative=False)
            section = Section.create(section, title=sec.text)
            if section:
                sections.append(section)
        return sections

    def remove_noise(self) -> None:
        bbox_main_text = self.bbox_main_text
        if bbox_main_text is None: return None
        try:
            page = self.page.crop(bbox_main_text, relative=False)
        except Exception as e:
            print(e)
            x0, top, x1, bottom = bbox_main_text
            bbox_main_text = x0, top, self.page.width, bottom
            page = self.page.crop(bbox_main_text, relative=False)
        self.page = page

    def search(self, regex: Pattern, en_only=True) -> Union[Match, None]:
        text = re.sub(r'[^\x00-\x7F]+', '', self.text) if en_only else self.text
        return re.search(regex, text, flags=re.IGNORECASE | re.MULTILINE)

    def get_section(self, regex: Pattern) -> list:
        sections = self.sections
        return [section for section in sections if re.search(regex, section.title, flags=re.IGNORECASE)]

    def create_section(self, sec_bbx, title=None, relative=False):
        sec = self.page.within_bbox(sec_bbx, relative=relative)
        return Section.create(sec, title=title)

    def divide_into_two_cols(self, d=0.5, relative=True):
        l0, l1 = 0 * float(self.page.width), d * float(self.page.width)
        r0, r1 = d * float(self.page.width), 1 * float(self.page.width)
        top, bottom = 0, float(self.page.height)
        l_bbx = (l0, top, l1, bottom)
        r_bbx = (r0, top, r1, bottom)

        left_col = self.create_section(
            self, l_bbx, title='Left Column', relative=relative)
        right_col = self.create_section(
            self, r_bbx, title='Right Column', relative=relative)
        return left_col, right_col

    
    def to_bilingual(self):
        return Page_bilingual.create(self.page)

    def __repr__(self):
        return f'<{self.__class__.__name__} {self.page_number}>'


class Page_bilingual(Page):
    def __init__(self, page):
        super().__init__(page)
    
    @property
    def df_feature_text(self) -> pd.DataFrame:
        df_ft = self.df_char[~self.df_char['fontname'].isin(self.main_fontname)]
        df_feature_text = df_ft.groupby(['top', 'bottom', 'fontname', 'size']).agg({
            'x0': 'min', 
            'x1': 'max', 
            'text': ''.join
            }).reset_index()
        if df_feature_text.empty:
            return df_feature_text
        df_feature_text = df_feature_text[df_feature_text.text.str.contains(r'\w+')]
        return df_feature_text

    @property
    def df_section_text(self) -> pd.DataFrame:
        self.df_lang = 'en'
        df_en = self.df_feature_text
        self.df_lang = 'cn'
        df_cn = self.df_feature_text
        self.df_lang = None
        
        def df_section_text(df):
            if df.empty:
                return df
            title_interval = df['bottom'].shift() - df['top']
            indicator = (title_interval.abs() > df['size'] + 1).cumsum()
            df_section_text = df.groupby(indicator).agg({
                'top': 'first',
                'bottom': 'last',
                'fontname': 'first',
                'size': 'first',
                'x0': 'first',
                'x1': 'first',
                'text': ''.join
            })
            return df_section_text
        
        df_section_text = df_section_text(df_en).append(df_section_text(df_cn))
        if df_section_text.empty:
            return df_section_text
        df_section_text.sort_values(by=['top'], inplace=True)
        df_section_text['next_top'] = df_section_text.top.shift(-1)
        df_section_text.fillna(self.page.height, inplace=True)
        return df_section_text
    
    @property
    def df_page_top_feature_text(self):
        df = self.df_section_text
        if df.empty:
            return df
        return df[df.top/df.bottom.max() < 0.2]

    @property
    def col_division(self) -> Union[None, float]:
        df_section_text = self.df_section_text
        if df_section_text.empty:
            return None
        df_section_text = df_section_text[df_section_text.top > self.df_page_top_feature_text.top.max()]
        if df_section_text.empty:
            df_section_text = self.df_section_text
        min_x0 = df_section_text.x0.min()
        max_x0 = df_section_text.x0.max()
        buffer = self.main_fontsize.min()
        x0, top, x1, bottom = self.bbox_main_text
        if min_x0 != max_x0:
            col_division = float(max_x0) - float(buffer)
            print(f'There is another colmun divided at {float(col_division)}.')
            if col_division > x0:
                return col_division
        return None


class Section(Page):
    def __init__(self, page, title=None):
        super().__init__(page)
        self.title = title

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self.title}>'


class Outline:
    
    def __init__(self, title, page_range, pb_pdf):
        self.title = title
        self.page_range = page_range
        self.pb_pdf = pb_pdf
    

    @property
    def pages(self):
        page_range = self.page_range
        pb_pdf = self.pb_pdf
        return [Page.create(pb_pdf.pages[p]) for p in page_range]

    @property
    def page_range(self) -> list:
        from_page, to_page = self.from_page, self.to_page
        if from_page and to_page:
            return list(range(from_page, to_page + 1))
        return []

    @page_range.setter
    def page_range(self, page_range):
        if type(page_range) is not tuple or not page_range:
            raise TypeError(f'page_range type {type(page_range)} is not tuple')
        self._page_range = page_range

    @property
    def title(self) -> str:
        return self._title

    @title.setter
    def title(self, title):
        self._title = utf8_str(title).capitalize()

    @property
    def from_page(self):
        return min(self._page_range, default=None)

    @property
    def to_page(self):
        return max(self._page_range, default=None)

    def __repr__(self):
        return f'<{self.__class__.__name__}: {self.title} {self.from_page} - {self.to_page}>'

class PageWithSection:
    
    section_regex = r''
    
    def __init__(self, pages):
        self.pages = pages
    
    @classmethod
    def retrieve(cls, pages):
        page_range = []
        for page in pages:
            if page.df_feature_text.empty:
                continue
            dfs_feature_text = page.df_feature_text[page.df_feature_text.text.str.contains(cls.section_regex, flags=re.IGNORECASE)]
            if not dfs_feature_text.empty:
                page_range.append(page.page_number)
        related_pages = [page for page in pages if page_range and page.page_number in range(min(page_range), max(page_range) + 1)]
        return cls(related_pages) if related_pages else None
    
    def __repr__(self):
        return f'<{self.__class__.__name__}>'

class ReportOutline:

    title_regex = r''
    
    def __init__(self, outline):
        self.pages = outline
    
    @classmethod
    def create(cls, outline):
        fail_conditions = {
            isinstance(outline, Outline) is False: lambda err_msg: TypeError(err_msg + f'argument must be an Outline instance.'),
            not any(outline.pages): lambda err_msg: ValueError(err_msg + f'outline instance must have at least one page.'),
            re.search(r'Global|Local search', outline.title) is None and re.search(cls.title_regex, outline.title, flags=re.IGNORECASE) is None: lambda err_msg: ValueError(err_msg + f'outline title: {outline.title} does not match with: title_regex {cls.title_regex}.'),
        }
        create_failed = fail_conditions.get(True, False)
        if create_failed:
            raise create_failed(f'Failed to initialise {cls.__name__} instance.')
        return cls(outline)

    @property
    def pages(self):
        return self._pages

    @pages.setter
    def pages(self, outline):
        self._pages = [page for page in outline.pages if page]
    
    @property
    def from_page(self):
        return min([page.page_number for page in self.pages])
    
    @property
    def to_page(self):
        return max([page.page_number for page in self.pages])
    
    def __repr__(self):
        return f'<{self.__class__.__name__} p.{self.from_page} - {self.to_page}>'

if __name__ == '__main__':
    pass
    # from hkex_api import HKEX_API
    # Logger.show_stream_log()
    # api_query = HKEX_API()
    # datas = api_query.get_data()
    # urls = [data.file_link for data in datas]
    # bad_urls = [data.file_link for data in datas if not data.file_link.endswith('pdf')]
    # for url in urls:
    #     pdf = PDF.create(url)
    # pdf = PDF.create('/Users/macone/Documents/cs_project/toc/2020073000624.pdf')