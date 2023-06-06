# Common Crawler

'Common Crawl' (https://commoncrawl.org/) is an open repository of years of internet webpages stored for use. The data can be difficult to work with when all that is desired is plaintext. 

We provide a lightweight and simple python utility for collecting and batching plaintext data from common crawl in a multiprocessing manner. The data is cleaned, if desired, before being returned.

Data from Common Crawl comes in .WARC, .WET, and .WAT formats, as described here (https://commoncrawl.org/the-data/get-started/). The .WARC files store the information of the crawl itself (responses, request information, etc.). The .WET files store the plaintext. The .WAT files store metadata about the .WARC files. Note that data before 2018 does NOT store language information. Data before 2018 will need language detected manually. 

**This repository specifically focuses on converting common crawl plaintext to a usable dataset format.**

** PLEASE NOTE THAT THE REPOSITORY IS IN ACTIVE DEVELOPMENT AS A SIDE PROJECT, development is slow **

# Install

# Usage