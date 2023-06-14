'''
By George Witt || June 2023

This file has the class implementations for the various managers and
processes that will be run. 
'''
from .processes import DownloadProcess
from typing import Callable, List

class DownloadManager:
    '''
    The download manager is the primary interface point
    for the library. It handles processing all the user's requests
    for how to format the final data, and setting up the sub-processes.
    
    The download manager splits its work of downloading the requested amount of data onto
    as many processers as possible. Each processor is informed separately of the 
    amount of data it will have to handle. Initial estimates are made with the assumption
    that each individual unzipped wet file will be approximately 300 mb. This should be a 
    fairly conservative high bound estimate for the size. Extra data will be pruned, the
    final datasets may be *slightly* smaller than requested.

    The download manager may be used ...
    * to generate samples directly for immediate use in training
    * to generate a text dataset of some kind, with custom preprocessing added
    '''

    ESTIMATED_SLICE_SIZE = 300 # Units of MB

    supported_modes = ['IMMEDIATE']

    def __log(self, text:str) -> None:
        '''
        Utility private function meant to help handle logging
        @text: String text to display
        '''
        if self.b_logging:
            print(text)

    def __check_input(self, num_index_proc:int|None =None, 
                      size_of_desired_data:int|None =None) -> None:
        '''
        Utility function to assert that all given parameters are valid. May be used again if any
        parameters are changed. DO use this to make sure everything you are using is logical, otherwise
        more data than intended may be downloaded.

        @num_index_proc: Integer number of processors to use per index.
        @size_of_desired_data: Integer count in MB of how much data should be downloaded. Each slice is 
        estimated at 300 MB. This choice must be consistent with your choice in number of downloads. If not,
        the number of download processes you are using will be automatically reduced if possible.
        @return None
        
        Will raise an exception if something goes wrong.
        '''

        if num_index_proc is None:
            num_index_proc = self.num_index_proc
        if size_of_desired_data is None:
            size_of_desired_data = self.size_of_desired_data

        assert self.mode in DownloadManager.supported_modes

        try:
            assert num_index_proc * DownloadManager.ESTIMATED_SLICE_SIZE >= size_of_desired_data
        except AssertionError:
            self.__log("PROVIDED PROCESSOR SIZES ARE INCONSISTENT WITH REQUESTED MEMORY SIZE, REBALANCING PROCESSORS")
            
            # To balance, we will keep the number of download processes the same.
            num_index_proc = size_of_desired_data / (DownloadManager.ESTIMATED_SLICE_SIZE)
            assert num_index_proc >= 1
            self.num_index_proc = int(num_index_proc)

    def __prepare_indices(self) -> List[str]:
        '''
        Utility function meant to download the indices list to help construct the download 
        URLS for the .wet files.
        '''

    def __spawn_off_downloads(self, custom_size:int|None =None, indices_lst:List[str]|None =None) -> None:
        '''
        This function begins spawning off download processes. To do this, it needs a list of all the indices. 
        This can be passed, or it will be found. The sorting function will be called on the list of indices, and must
        return a list of booleans for each index. If there is no sorting function, downloads will occur from there.

        @custom_size: Integer size in MB of files to download, randomly chosen.
        @indices_lst: List of strings representing the indices to search if manually specified.
        '''
        if indices_lst is None:

            # If no indices are specified, we will pull down the index file
            indices_lst = self.__prepare_indices()
            self.download_processes = [] # TODO: IN PROGRESS
            #for item in indices_lst:
            #    p = DownloadProcess(item)
            #    p.start()
            #    self.download_processes.append(p)

    def __init__(self, mode:str ='IMMEDIATE', num_index_proc:int =5, 
                 size_of_desired_data:int|None =None, wait_time:float|None =None, sort_func:Callable|None =None,
                 preprocessing_func:Callable|None =None, exit_func:Callable|None =None, b_logging:bool =False):
        '''
        Initialize the manager with information on desired sort characteristics, desired end data format,
        the current mode of the manager, number of processes to use, etc. etc.

        @mode: String that must be within DownloadManager.supported_modes. Represents the usage of the download 
        manager. Potentially for immediate sampling in a program, building an external dataset, etc. 
        @num_index_proc: Integer number of processors to use per index.
        @size_of_desired_data: Integer count in MB of how much data should be downloaded. Each slice is 
        estimated at 300 MB. This choice must be consistent with your choice in number of downloads. If not,
        the number of download processes you are using will be automatically reduced if possible.
        @wait_time: Float amount of time to wait between process downloads, will be automatically chosen if 
        receiving 503 reduce response rate requests. By the common crawl terms of service, it is necessary that
        response rates be optimized as to minimize the number of calls to their servers.
        @preprocessing_func: Callable function meant to further process text when it is cleaned and returned.
        @exit_func: Function called on exit if you'd like to customize the return value in some way. Whatever
        your function returns, the manager will return.
        @b_logging: Boolean of whether or not to display and perform logging.
        '''

        self.mode = mode
        self.num_index_proc = num_index_proc
        self.size_of_desired_data = size_of_desired_data
        self.wait_time = wait_time
        self.sort_func = sort_func
        self.preprocessing_func = preprocessing_func
        self.exit_func = exit_func
        self.b_logging = b_logging

        self.download_processes = []

        if b_logging:
            self.__log("** INITIALIZING DOWNLOAD MANAGER **")
            self.__log("Usage, recall that...")
            self.__log("MODE must be a valid string in supported modes")
            self.__log("The processes must add up to the desired size correctly")
            self.__log("*******************************************************")

        # Check the input
        self.__check_input()

    def get_batch(self, custom_size:int|None =None, **kwargs):
        '''
        Get batch of data with a custom size. This should be used for live batches in 
        the live mode of the class, or this is one of many steps used in building a 
        database offline.

        @custom_size: Integer size of data to download in MB, like in initialization. 
        None implies the same size as in initialization. 
        @kwargs: Any necessary kwargs for the exit function.
        '''
        if custom_size is not None:
            self.__check_input(size_of_desired_data=custom_size)
        else:
            custom_size = self.size_of_desired_data
        
        res = self.__spawn_off_downloads(custom_size)

        if not self.exit_func is None:
            return self.exit_func(res, **kwargs)
        else:
            return res