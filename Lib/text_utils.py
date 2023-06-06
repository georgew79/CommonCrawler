'''
By: George Witt || June 2023

Utility functions meant to help with cleaning text
up for model processing

'''
import re
import warc

from re import sub
from typing import Callable, List
from tqdm import tqdm

def __process_lines(text:str, language_reg:str) -> str:
    remove_nonprint = re.compile(language_reg)
    return sub(remove_nonprint, '', text)

def default_filter(single_string:str) -> str:
    '''
    This is a default filter function meant to help do some basic filtering tasks.
    Please feel free to implement your own. This is only meant to be used in the case 
    of the list string approach.

    @single_str: The single string to check and filter in/out
    @return: The string to push
    '''

    if single_string.isspace():
        return None
    else:
        return single_string

def remove_html(text:str) -> str:
    remove_html_reg = re.compile('<.*?>')
    return sub(remove_html_reg, '', text)

def clean_text(text:str, b_remove_html:bool =True, b_rm_nprintable:bool =True, b_split_nlines:bool =False, 
               func:Callable|None =None, language_reg:str ='[^a-zA-Z ]', **kwargs) -> str:
    '''
    Function meant to handle cleaning text from common crawl. 
    @text: String meant to represent the text to clean
    @b_remove_html: boolean of whether or not to remove any HTML information
    @b_rm_nprintable: boolean of whether or not to remove any non printable characters
    @b_split_nlines: boolean of whether or not to split by '\n', (whether or not to include \n in the string)
    @func: Extra preprocessing function pointer that you may wish to add.
    Add any extra kwargs for the func as necessary. They will be passed in the kwargs dictionary.
    I do assume that the text is the first argument to the function.
    NOTE: Any alterations in the return type of the function you give will ALTER the return type of this function.
    @language_reg: String for a regular expression pattern of what constitutes the language (ie what characters
    to keep)

    @return: Return one long string.
    '''
    if b_split_nlines:
        language_reg = language_reg[:-1]
        language_reg += '\n]'

    if b_remove_html:
        text = remove_html(text)
    
    if b_rm_nprintable:
        text = __process_lines(text, language_reg)
    
    if func is not None:
        text = func(text, **kwargs)
    
    return text

def process_wet(unzipped_path:str, standard_encoding:str ='utf-8', error_handling:str ='ignore', 
                should_capture:bool =False, should_split:bool =False,
                filter_func:Callable|None =None, **kwargs) -> str|List[str]:
    '''
    This function process a .wet unzipped file, and starts processing the underlying text. 

    @unzipped_path: Where is the unzipped file?
    @standard_encoding: The encoding method to use for decoding the byte string.
    @error_handling: 'ignore', 'replace', or 'strict'. Specifies how to handle decoding errors.
    @should_split: Flag for splitting text by newlines. 
    @should_capture: Flag for capturing keyboard interrupts. Adding text can take a long
    time, so if should_capture is true then a keyboard interrupt will just return
    the text received so far.
    @filter_func: Callable function that takes in a single string meant to help filter EITHER
    the whole string at once (if that is what you've selected), or the string. Return the item.

    @return: Returns a cleaned string.
    '''
    text = ""
    count = 0
    try: 
        with warc.open(unzipped_path) as f:
            for i, record in enumerate(tqdm(f)):
                raw = record.payload.read().decode(standard_encoding, error_handling)
                cleaned = clean_text(raw, b_split_nlines=should_split, **kwargs)
                text += cleaned
                count += 1
    except KeyboardInterrupt:
        if not should_capture:
            raise KeyboardInterrupt()
    
        print(f"Processed {count} blocks")

    if should_split:
        if filter_func is not None:
            ret = list(filter(filter_func, text.split('\n')))
        else:
            ret = list(filter(None, text.split('\n')))
        return ret
    else:
        if filter_func is not None:
            return filter_func(text)
        else:
            return text