**This project aims to harvest YouTube data and store it in a data warehouse using SQL, MongoDB, and Streamit.**


The following steps are involved in the project:

**1. Harvesting YouTube data:** This can be done using the YouTube Data API.
**2. Storing the data in a data warehouse:** The data can be stored in a data warehouse such as PostgreSQL or MySQL.
**3. Processing the data in real time:** This can be done using a stream processing engine such as Apache Kafka.
**4. Analyzing the data:** The data can be analyzed using a variety of tools such as Tableau or Power BI.

**Requirements**
Python 3.6+
PostgreSQL
MongoDB
Streamit
YouTube Data API v3
Setup

**Install the required Python packages:**
pip install sqlalchemy psycopg2 psycopg2-binary pymongo streamit youtube-data-api

**Create a PostgreSQL database:**
psql -U postgres -c "CREATE DATABASE youtube_data_warehouse;"

**Start the Streamit server:**
streamit start

**Create a Streamit stream to process the YouTube data:**

**Run the Python script to harvest the YouTube data:**
python Youtube.py
The YouTube data will be stored in the PostgreSQL database and processed by the Streamit stream.
Analysis

For example, you could create a dashboard to track the number of views, likes, and comments for a particular video or channel. You could also use the data to identify trends in YouTube viewership or to discover new and emerging channels.

**Conclusion**
This project provides a framework for harvesting and warehousing YouTube data using SQL, MongoDB, and Streamit. The data can then be analyzed using a variety of tools to gain insights into YouTube viewership and trends.
