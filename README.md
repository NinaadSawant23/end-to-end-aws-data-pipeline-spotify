# Spotify AWS Data Pipeline

This project demonstrates how to build an end-to-end data pipeline using AWS services — S3, Glue, Athena, and QuickSight — to analyze Spotify data.

## 📊 Architecture


## 🚀 Technologies
- AWS S3
- AWS Glue (Visual + PySpark)
- AWS Athena
- AWS QuickSight
- IAM, Data Catalog, Parquet

## 🗂️ Dataset
- Spotify CSVs: albums, artists, tracks (preprocessed)
- Stored in S3 staging and processed to S3 data warehouse as Parquet

## 🧱 Pipeline Steps
1. Created IAM user and roles with fine-grained permissions
2. Created S3 bucket with `staging/` and `datawarehouse/` folders
3. Uploaded CSV files to `staging/`
4. Built visual ETL pipeline in AWS Glue Studio:
   - Output to Parquet in `datawarehouse/`
5. Ran Glue Crawler to register schema in Glue Catalog
6. Queried data with Athena
7. Created QuickSight dashboards for analytics

## 🖼️ Screenshots


## 📁 Sample Data

---

