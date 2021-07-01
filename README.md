# DW on Redshift with ETL
This project aims to build **OLAP Data Warehouse** for reporting and analytical purposes.

### Motivation
The following data warehouse is built for imaginary music streaming application called "Sparkify". The startup aims to establish consistent reporting service to analyze the activity of their users. Their data resides in S3 bucket in a directory of JSON logs as well as the directory of JSON metadata on their songs in the app.
### Objective
The main objective of the project is to build ETL pipeline to extract data from S3 and stage them in Redshift. Afterwards, the data in Redshift must be transformed into a set of dimensional tables that work best for read-intensive purposes.
### Datasets 
<p>There two basic log files in S3 bucket:</p> 
<ul> <li> song_data (song metadata)</li>
<li> log_data (event log)</li></ul>

<p>OLAP Database schema is a star schema optimized for queries on song play analysis. It consists of the following entities </p>
<ol><li> <b>songplays</b> - fact table, records in event data associated with song plays</li>
<li><b>users</b> - users in the app</li>
<li><b>songs</b> - songs in the music database</li>
<li><b>artists</b> - artists in the music database</li>
<li><b>time</b> - timestamps of records in songplays broken down into specific units: <em>start_time, hour, day, week, month, year, weekday</em></li>
</ol>

### Project files
<code>create_table.py</code> - where fact table and dimension tables are created for the star schema in the Redshift
<code>etl.py</code> - where the data load from S3 to Redshift staging tables takes place. Then the data is further processed into analytics tables on Redshift
<code>sql_queries.py</code> - where all SQL queries for creating needed tables, dropping tables and loading data are created.
<code>README.md</code> - readme file with instructions

### Sample queries
<p>Run the following queries to check the information in tables:</p>
<code>SELECT * FROM column_name;</code>
<p>Run the following query to check table_name, column_name and data_type:</p>
<code>SELECT<br>table_name, column_name, data_type <br> FROM <br> information_schema.columns<br>
WHERE <br>table_name = 'table_name'</code>

