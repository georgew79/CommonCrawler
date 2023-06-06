'''
By: George Witt || May 2023

Utility functions meant to handle pulling files down 
with particular parameters.

'''
import requests
import sys
import gzip
import warc
import shutil
import os

from tqdm import tqdm
from text_utils import clean_text

class ResponseException(Exception):
    ''' Raised when there is some issue in the response.'''
    def __init__(self, res_message: str):
        self.res_message = res_message
        super().__init__(f'Response failed with message {res_message}')

def download_file(path:str, save_file_path:str ='NONAME', unzipped_path:str = 'UNZIPPED', should_print:bool = False) -> None:
    '''
    This function pulls down a general file with the given path, saves the gz version, then unzips it for processing.
    NOTE: No assumptions are made about the save_file type and the unzipped_file type, so it is absolutely crucial
    that these variables are provided with the correct file extensions.

    @path: Path for .wet file, MUST be a valid path. 
    @save_file_path: Choose where to save the file as text for processing. This is only the intermediary save.
    @unzipped_path: Choose where to save the unzipped file.
    @should_print: Flag for extra printing, if it's available.
    '''

    # Get file with request
    r = requests.get(path, stream=True)
    if not r.ok:
        raise ResponseException(f"REQUEST FOR {path} FAILED WITH CODE {r.status_code}, MEANING {r.reason}")
    
    # Got file, now write it to disk.
    total = int(r.headers.get('content-length', 0))
    try:
        with tqdm(total=total, unit='iB', unit_scale=True, unit_divisor=1024) as bar:
            with open(save_file_path, 'wb') as f: 
                for data in r.iter_content(chunk_size=1024):
                    f.write(data)
                    bar.update(sys.getsizeof(data))
    except KeyboardInterrupt:
        print(f"QUITTING DOWNLOAD for file {path}")

    with gzip.open(save_file_path, 'rb') as f_in:
            with open(unzipped_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
    os.remove(save_file_path)

def download_wet_file(path:str, standard_encoding:str ='utf-8', error_handling:str ='ignore', 
                    save_file_path:str ='NONAME.warc.wet.gz', unzipped_path:str = 'UNZIPPED.warc', should_print:bool =False) -> str:
    '''
    This function pulls down the .wet gz file with the given filepath. The filepath 
    provided **must** be a valid path to a common crawl .wet file.
    
    @path: Path for .wet file, MUST be a valid path. 
    @standard_encoding: The encoding method to use for decoding the byte string.
    @error_handling: 'ignore', 'replace', or 'strict'. Specifies how to handle decoding errors.
    @save_file_path: Choose where to save the file as text for processing. This is only the intermediary save.
    @unzipped_path: Choose where to save the unzipped file.
    @should_print: Flag for extra printing, if it's available.
    @return: Returns a cleaned string.
    '''
    #if save_file_path == 'NONAME.warc.wet.gz' or unzipped_path == 'UNZIPPED.warc':
    #    raise UserWarning(f"WARNING, did not specify save location for file, using {save_file_path} and {unzipped_path} in local directory")

    download_file(path=path, save_file_path=save_file_path, unzipped_path=unzipped_path, should_print=should_print)

    # Written to disk, now get raw text.
    text = ""
    with warc.open(unzipped_path) as f:
        for i,record in enumerate(tqdm(f)):
            text += clean_text(record.payload.read().decode(standard_encoding, error_handling))
    
    return text

def pull_from_paths(index_path: str, save_path: str, should_print=False):
    '''
    This function downloads randomly selected files from within an index slice that was 
    previously selected.
    '''

    pass