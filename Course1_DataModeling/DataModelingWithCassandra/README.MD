# 🎯 Cassandra Data Modeling Project

This project demonstrates query-based data modeling using Apache Cassandra.  
Three different tables are created to support three specific queries based on the event data of a music streaming app.

---

## 📁 Project Structure

- `event_datafile_new.csv`: Cleaned dataset used for table population.
- `project_notebook.ipynb`: Jupyter Notebook with all table creation, data insertion, and queries.
- `README.md`: Project documentation (this file).

---

## 🧠 Project Goal

Design Apache Cassandra tables based on the "one query = one table" principle.  
The project focuses on:
- Choosing appropriate partition and clustering keys
- Writing efficient and targeted `SELECT` statements
- Avoiding `ALLOW FILTERING`
- Using proper data types and column ordering

---

## 📊 Queries & Table Designs

### 1. Retrieve the artist, song name, and song length for session ID = 338 and itemInSession = 4.

- **Table Name:** `songs_by_session`
- **Primary Key:** `(session_id, item_in_session)`
- **Purpose:** Enables fast access to a specific song play by session and item order.

### 2. Retrieve artist, song, and user (first & last name) for user ID = 10 and session ID = 182, ordered by itemInSession.

- **Table Name:** `user_session_playlist`
- **Primary Key:** `((user_id, session_id), item_in_session)`
- **Purpose:** Retrieves the full playlist of a user session in playback order.

### 3. Retrieve every user (first and last name) who listened to the song 'All Hands Against His Own'.

- **Table Name:** `song_listeners`
- **Primary Key:** `(song_name, user_id)`
- **Purpose:** Retrieves unique users who listened to a specific song.

---

## ⚙️ How to Run

1. Clone the repository or open the notebook in Jupyter Lab.
2. Ensure Apache Cassandra is running locally.
3. Execute each cell in order:
   - Create keyspace and connect to Cassandra
   - Create tables
   - Insert data from `event_datafile_new.csv`
   - Run queries
4. Review output for correctness.

---

## ✅ Key Improvements Implemented

- Renamed tables to reflect their purpose
- Used proper data types (e.g., `song_length` as `float`)
- Avoided use of `SELECT *`
- Added markdown explanations for each table and query

