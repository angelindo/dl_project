Description: The ETL goal is to provide an organized data ready to analytics on a Datalake for Sparkify.

Files:
dl.cfg: Configuration file with credentials
etl.py: Main script which will fill the data on all repositories

How to run:
Change if necesarry the credentials on dl.cfg
Then, run etl.py

Database schema:
There are five interfaces, each of them have their own route to unload their parquet files, to have an order because, every interface has diferent partitions or none partitions at all. So, at the moment to use (read) these files you can do it clean and organized.