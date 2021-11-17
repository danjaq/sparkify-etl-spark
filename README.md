# Data Lake
An ETL pipeline to analyze the listening behavior of Sparkify's users, built for Udactiy's Data Engineering Nanodegree. 

## Overview
This ETL pipeline combines two data sets: 1) a subset of the [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/) and 2) simulated user logs from a music streaming service. Both datasets are stored on S3 as JSON files. Using Spark, the data is read from there, assembled into tables and written back out to s3 as parquet files. These tables use a star schema for the benefit of simplifying the queries needed. The fact table is `songplays` and the following are the dimension tables: `time`, `users`, `songs`, and `artists`.

## Files
- etl.py : Python script to create the full pipeline.
- README.md: This file
- dl.cfg : Config file for AWS access warehouse.

## Run Instructions
1. Edit d;.cfg appropriately for your credentials.
2. Run `etl.py` to import the data from S3 JSON files and assembled the tables.
3. Query as needed.