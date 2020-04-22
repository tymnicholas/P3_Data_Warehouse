# Data Warehouse - AWS Redshift

### Intro

In this project, an ETL pipeline for a database hosted on Redshift will be built. This project will utilise AWS Redshift as well as data warehouse concepts.


### Goal


Build an ETL pipeline that extracts Sparkify data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for Sparkify's analytics team to continue finding insights in what songs their users are listening to



### Dataset

The dataset is from [Million Song Dataset](https://labrosa.ee.columbia.edu/millionsong/).

Below is an example of what a single song file looks like.
>{"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}


## Database Schema
    ![Dimension and Fact Table Diagram](/P3_Diagram.png)



## How to run
    1. Open dwh.cfg and fill up the **key** and **secret** field. **host** and **arn** are to be left empty.
    2. Open P3.ipynb.
    3. Execute steps in notebook.
        - After executing the *Set up Redshift cluster* step, some time is need for the cluster to start.
        - Execute *Describe Redshift cluster for AVAILABLE status* to observe cluster status and wait till it is AVAILABLE.
        - Execute *Update Endpoint and role ARN to Config file*. Note that the **host** and **arn** fields in dwh.cfg are now updated with the Redshift's instance details.
        - Execute the codes below to create the fact and dimension tables
        >%%time
        >%run create_tables.py
        - Execute the codes below to perform the ETL steps
        >%%time
        >%run etl.py
    4. Perform SQL queries as desired.
    5. Clean up and delete cluster
            
        
        