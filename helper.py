from itertools import groupby
from operator import itemgetter
import re

def consecutive_int_list(li):
    '''
    return a list of list of consecutive integers
    '''
    return [list(map(itemgetter(1), g)) for k, g in groupby(enumerate(li), lambda xi: xi[0]-xi[1])]

def utf8_str(string: str) -> str:
    '''
    ensure string has utf-8 encoding
    '''
    if not isinstance(string, str):
        string = string.decode('utf-8', errors="ignore")
    return re.sub(r'\r|\n|\ufeff', '', string)

def flatten(li: list) -> list:
    '''
    flatten a irregular list recursively;
    for flattening multiple levels of outlines.
    '''
    return sum(map(flatten, li), []) if isinstance(li, list) else [li]


