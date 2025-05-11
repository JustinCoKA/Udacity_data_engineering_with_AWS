# 🎵 Sparkify Data Warehouse Project

![AWS](https://img.shields.io/badge/AWS-Redshift-orange?logo=amazonaws)
![S3](https://img.shields.io/badge/Storage-S3-blue?logo=amazon-s3)
![Python](https://img.shields.io/badge/Python-3.8+-brightgreen?logo=python)
![ETL](https://img.shields.io/badge/Pipeline-ETL-blueviolet)
![License](https://img.shields.io/badge/License-MIT-lightgrey)

## 📌 Overview

Sparkify is a music streaming startup that has grown rapidly and needs a scalable data warehouse solution. This project builds an **ETL pipeline using AWS Redshift** to extract data from S3, stage it, and transform it into a **star-schema** optimized for analytics. It enables the Sparkify analytics team to uncover insights like user engagement, song popularity, and usage trends.

## 🏗️ Architecture

```
S3 (JSON logs + metadata)
  ├── song_data
  ├── log_data
  └── log_json_path.json

        ↓ COPY

Redshift Staging Tables
  ├── staging_events
  └── staging_songs

        ↓ INSERT

Redshift Star Schema
  ├── Fact Table: songplays
  ├── Dimension Tables:
      ├── users
      ├── songs
      ├── artists
      └── time
```

## 🧱 Schema Design

### Fact Table: `songplays`
- `songplay_id`, `start_time`, `user_id`, `level`, `song_id`, `artist_id`, `session_id`, `location`, `user_agent`

### Dimension Tables
- `users`: `user_id`, `first_name`, `last_name`, `gender`, `level`
- `songs`: `song_id`, `title`, `artist_id`, `year`, `duration`
- `artists`: `artist_id`, `name`, `location`, `latitude`, `longitude`
- `time`: `start_time`, `hour`, `day`, `week`, `month`, `year`, `weekday`

## ⚙️ ETL Process

1. **COPY (S3 → Redshift)**  
   Raw JSON logs are loaded into staging tables using the Redshift `COPY` command.

2. **INSERT (Staging → Analytics Tables)**  
   - Filters events with `page = 'NextSong'`  
   - Joins with songs data on title and artist  
   - Timestamps converted using `TIMESTAMP 'epoch' + ts / 1000 * interval '1 second'`  
   - Time dimensions extracted

## 📁 Project Structure

```
CloudDataWarehouse/
├── main.py
├── config/
│   └── dwh.cfg
├── etl/
│   └── etl.py
├── sql/
│   ├── sql_queries.py
│   └── create_tables.py
├── infrastructure/
│   └── aws_setup.py
└── README.md
```

## 🚀 How to Run

### 1. Setup Config

`config/dwh.cfg`

```ini
[CLUSTER]
CLUSTER_IDENTIFIER=...
HOST=...
DB_NAME=...
DB_USER=...
DB_PASSWORD=...
DB_PORT=...
NODE_TYPE=...
REGION=...

[IAM_ROLE]
IAM_ROLE_NAME=...
ARN=...

[S3]
LOG_DATA='s3://udacity-dend/log_data'
LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
SONG_DATA='s3://udacity-dend/song_data'
```

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 3. Run the project

```bash
python main.py
```

Use the menu:

```
1 - Create IAM Role
2 - Create Redshift Cluster
3 - Check Cluster Status
4 - Create Tables
5 - Run ETL
6 - Run All Steps
7 - Delete Cluster
0 - Exit
```

## 🔍 Sample Analytical Queries

```sql
SELECT s.title, COUNT(*) AS play_count
FROM songplays sp
JOIN songs s ON sp.song_id = s.song_id
GROUP BY s.title
ORDER BY play_count DESC
LIMIT 5;

SELECT level, COUNT(DISTINCT user_id)
FROM songplays
GROUP BY level;

SELECT hour, COUNT(*) AS plays
FROM time
GROUP BY hour
ORDER BY plays DESC;
```

## 📌 Requirements

```
boto3>=1.28.0
psycopg2-binary>=2.9.5
configparser>=5.3.0
```

## ⚠️ Notes

- Don't forget to delete your Redshift cluster when done to avoid charges.
- Timestamps are converted using:
  `TIMESTAMP 'epoch' + ts / 1000 * interval '1 second'`

## 👤 Author

Developed as part of the **Udacity Data Engineering Nanodegree**.  
Inspired by real-world data warehousing applications on AWS.
