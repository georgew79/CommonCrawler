'''
By: George Witt || June 2023

Utility functions meant to help with cleaning text
up for model processing

'''
import re

from re import sub
from typing import Callable, List

def __process_lines(text:str, language_reg:str) -> str:
    remove_nonprint = re.compile(language_reg)
    return sub(remove_nonprint, '', text)

def remove_html(text:str) -> str:
    remove_html_reg = re.compile('<.*?>')
    return sub(remove_html_reg, '', text)

def clean_text(text:str, b_remove_html:bool =True, b_rm_nprintable:bool =True, b_split_nlines:bool =False, 
               func:Callable|None =None, language_reg:str ='[^a-zA-Z0-9 _]', **kwargs) -> str|List[str]:
    '''
    Function meant to handle cleaning text from common crawl. 
    @text: String meant to represent the text to clean
    @b_remove_html: boolean of whether or not to remove any HTML information
    @b_rm_nprintable: boolean of whether or not to remove any non printable characters
    @b_split_nlines: boolean of whether or not to split by '\n'
    @func: Extra preprocessing function pointer that you may wish to add.
    Add any extra kwargs for the func as necessary. They will be passed in the kwargs dictionary.
    I do assume that the text is the first argument to the function.
    NOTE: Any alterations in the return type of the function you give will ALTER the return type of this function.
    @language_reg: String for a regular expression pattern of what constitutes the language (ie what characters
    to keep)

    @return: Either return one long string with the text, or a list separated by newlines.
    '''

    if b_remove_html:
        text = remove_html(text)
    
    if b_rm_nprintable:
        text = __process_lines(text, language_reg)
    
    if func is not None:
        text = func(text, **kwargs)

    if b_split_nlines:
        return text.split('\n')
    
    return text

