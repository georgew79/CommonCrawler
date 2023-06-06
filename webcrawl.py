'''
By: George Witt || May 2023

Main file for handling pulling down data as desired. This is the starting point
for the script. Separation of tasks occurs from here. 

Overall the process can be visualized as follows:

1) Sorting information is provided, including:
    a) URL regular expressions
    b) Date ranges
    c) Languages
    NOTE: search information may be left to none.
2) Parameters for algorithm are provided, including:
    a) Linear crawl or Random crawl T/F
    b) Amount of data in GB 
3) Data is pulled randomly (or linearly), searched by index.
4) Once the correct indices are found, the .wet files are downloaded.
5) The .wet files are cleaned, processed, and returned in the desired format.
'''