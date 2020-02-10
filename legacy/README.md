# extract-crawler
The Globus, POSIX and S3 crawlers for instantiating metadata extraction jobs over files.

Update 02/10/2020: 

The code in here is deprecated and needs to be converted to its own series of funcX functions. 
For instance, we could add a 'decompress widget' to an extractor that inflates a given file 
before extracting its metadata. We don't want to leave all files decompressed for space issues. 
