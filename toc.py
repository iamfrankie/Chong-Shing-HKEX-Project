from os import sys, path
sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from helper import flatten, get_title_liked_txt, consecutive_int_list, search_pattern_from_txt, unique
from get_pdf import _by_pypdf, _by_pdfplumber
import itertools, logging, re
from pdf import PDF
from typing import Union, Pattern, Match

class TableOfContent:
    
    audit_report_regex = r'^(?!.*internal)(?=.*report|responsibilities).*auditor.*$'
    corporate_gov_report_regex = r'corporate governance report'
    audit_fee_regex = r"AUDIT.*?REMUNERATION"

    # def __init__(self, src):
    #     super().__init__(src)
    def __init__(self, pdf_obj):
        # super().__init__(src)
        self.pdf_obj = pdf_obj
     
    @property
    def toc(self):
        with _by_pypdf(self.pdf_obj) as pdf:
            outlines = flatten(pdf.getOutlines())
            outlines, next_outlines = TableOfContent.current_next_outline_pairs(outlines)
            toc = {}
            for outline, next_outline in zip(outlines, next_outlines):
                title = TableOfContent.utf8_str(outline.title).title()
                from_page, to_page = TableOfContent.outline_page_range(pdf, outline, next_outline)
                try:
                    toc[title] = sorted([from_page, to_page])
                except TypeError as e:
                    logging.warning(f'{e}, from_page: {from_page}, to_page: {to_page}, both are {type(from_page)} which cannot be sorted')
                    continue
            return toc
    
    def search_outline_page_range(self, pattern:Pattern[str]):
        '''
        return searched title page range
        '''
        if self.toc:
            search_toc_result = self.search_outline_in_toc(pattern)
            return search_toc_result if search_toc_result else self.search_outline_in_pages(pattern)
        else:
            return self.search_outline_in_pages(pattern)
    
    # def search_outline_in_pages(self, pattern, verbose=False) -> list:
    #     '''
    #     return a list of pages number in tuples that contains pattern
    #     '''
    #     print('search by page!')
    #     pages = set()
    #     with _by_pdfplumber(self.pdf_obj) as pdf:
    #         for p, page in enumerate(pdf.pages):
    #             if verbose: print(f'searching p.{p}')
                
    #             try:
    #                 title_alike_txts = get_title_liked_txt(page)
    #             except KeyError:
    #                 logging.warning('Non textual page')
    #                 continue
    #             for txt in title_alike_txts:
    #                 if search_pattern_from_txt(txt, pattern):
    #                     pages.add(p)
    #                     if verbose: print(f'with pattern: found {txt} on p.{p}!')

    #         consecutive_pages = [tuple(li) for li in consecutive_int_list(unique(pages))]
    #         return consecutive_pages
        
    def search_outline_in_pages(self, pattern, page_range=None, size='fontname', verbose=False, show_matched=False) -> list:
        '''
        return a list of pages number in tuples that contains pattern
        '''
        print('search by page!')
        # print(f'pattern: {pattern}')
        pages = set()
        matched_pattern = []
        with _by_pdfplumber(self.pdf_obj) as pdf:
            if not page_range:
                page_range = pdf.pages
            else:
                page_range = [pdf.pages[p] for p in page_range]
            
            for page in page_range:
                p = page.page_number - 1
                # if verbose: print(f'searching p.{p}')
                
                try:
                    title_alike_txts = get_title_liked_txt(page, size=size)
                except KeyError:
                    logging.warning('Non textual page')
                    continue
                for txt in title_alike_txts:
                    if search_pattern_from_txt(txt, pattern):
                        pages.add(p)
                        matched_pattern.append(txt)
                        if verbose: print(f'with pattern: found {txt} on p.{p}!')

            consecutive_pages = [tuple(li) for li in consecutive_int_list(unique(pages))]
            if show_matched:
                return consecutive_pages, matched_pattern
            return consecutive_pages


    def search_outline_in_toc(self, pattern) -> list:
        '''
        return a list of matched title pattern page range
        '''    
        print('search by toc!')
        pages = []
        
        for outline, _page_range in self.toc.items():
            if re.search(pattern, outline, flags=re.IGNORECASE):
                from_page, to_page = _page_range
                page_range = list(range(from_page, to_page + 1))
                pages.append(page_range)
        pages = flatten(pages)
        consecutive_pages = [tuple(li) for li in consecutive_int_list(unique(pages))]
        return consecutive_pages

    

    @staticmethod
    def outline_page_range(pdf: object, outline: object, next_outline: object) -> tuple:
        try:
            from_page = pdf.getDestinationPageNumber(outline)
        except AttributeError:
            from_page = None
        try:
            to_page = pdf.getDestinationPageNumber(next_outline) - 1 if next_outline is not None else from_page
        except AttributeError:
            to_page = None
        from_page = from_page or to_page
        to_page = to_page or from_page 
        return from_page, to_page


    @staticmethod
    def utf8_str(string: str) -> str:
        '''
        ensure the title string is utf-8.
        '''
        if not isinstance(string, str):
            string = string.decode('utf-8', errors="ignore")
        return re.sub(r'\r|\n|\ufeff', '', string)
        
    
    @staticmethod
    def current_next_outline_pairs(outlines:list) -> tuple:
        outlines, next_outlines = itertools.tee(outlines, 2)
        next_outlines = itertools.chain(itertools.islice(next_outlines, 1, None), [None])
        return outlines, next_outlines

if __name__ == "__main__":
    import get_pdf
    from test_cases import test_cases
    from get_data import HKEX_API
    from helper import write_to_csv, n_yearsago, today
    # logging.basicConfig(level=logging.INFO)
    # query = HKEX_API(from_date=n_yearsago(n=1), to_date=today())
    
    
    # for data in query.data:
        
    #     result = {}
    #     url = data.file_link
    #     pdf = PDF(url)
    #     print(url)
    #     f = TableOfContent(pdf.pdf_obj)
    #     print()
    #     result['result'] = f.search_outline_page_range(TableOfContent.auditor_remunration_regex)
    #     result['url'] = url
    #     write_to_csv(result, 'result_2.csv')
    
    url = 'https://www1.hkexnews.hk/listedco/listconews/sehk/2020/0813/2020081300777.pdf'
    url = 'https://www1.hkexnews.hk/listedco/listconews/sehk/2020/0813/2020081300670.pdf'
    pdf = PDF(url)
    print(url)
    f = TableOfContent(pdf.pdf_obj)
    print(f.search_outline_page_range(TableOfContent.audit_fee_regex))