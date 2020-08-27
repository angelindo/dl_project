# Sparkify
* Sparkify is a music streaming startup, it has grown their user base and song database to the point that they want to move their data warehouse to a data lake, to speed up their analysis.

### Project summary: 
* This is a ETL project. Its goal is to provide an organized data ready to analytics on a Datalake for Sparkify, stored on a S3 bucket.

### Files:
* dl.cfg: Configuration file with credentials
* etl.py: Main script which will fill the data on all repositories

### How to run:
1. Change if necesarry the credentials on dl.cfg
2. Then, run etl.py

### Database schema:
* There are five interfaces, each of them have their own route to unload their parquet files, to have an order because, every interface has diferent partitions or none partitions at all. So, at the moment to use (read) these files you can do it clean and organized.

![ERD](https://udacity-reviews-uploads.s3.us-west-2.amazonaws.com/_attachments/339318/1586016120/Song_ERD.png)
###### Just reference, each table represents an interface
