# Data Lake with Spark

## Overview
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, you are tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.


## Project Structure
THis project contains the following files:
```
dl.cfg       
etl.py       
README.md
```

## Data Lake Schema
This data lake consists of a star schema, where `songplays` is the fact table in the center of the model. 
Surrounding dimensional tables are:`users`, `songs`, `artists`, and `time`.

### Fact Table
- **songplays** 
- Table with information on various events of songs being played by users:
    - `start_time`      - starting time of a song
    - `user_id`         - user ID
    - `user_level` - payed or free user
    - `song_id` - song ID
    - `artist_id` - artist ID
    - `session_id` - session ID
    - `user_location` - user location
    - `user_agent` - connection to the app
    - `ts_year` - year of songplay
    - `ts_month` - month of songplay
    - `songplay_id` - songplay ID

### Dimensional Tables
- **users** 
- App user information:
    - `userId` - user ID
    - `user_firstname` - firstname of user
    - `user_lastname` - lastname of user
    - `user_gender` - user gender
    - `user_level` - payed or free user

- **songs** 
- Song information:
    - `song_id` - song ID
    - `song_title` - song title
    - `song_year` - song release year
    - `song_duration` - song duration

- **artists** 
- Artist information:
    - `artist_id` - artist ID
    - `artist_name` - artist/band name
    - `artist_location` - artist/band location
    - `artist_latitude` - geographical information
    - `artist_longitude` - geographical information

- **time** 
- Timestamps of song plays:
    - `start_time` - start time of songplay
    - `ts_hour` - hour of songplay
    - `ts_day` - day of songplay
    - `ts_week` - week of songplay
    - `ts_month` - month of songplay
    - `ts_year ` - year- of songplay
    - `ts_weekday` - day of the week of songplay



## Project order
1. Add AWS IAM credentials in `dl.cfg`
2. Set output data path in the `main` function of `etl.py`
3. Run `etl.py`