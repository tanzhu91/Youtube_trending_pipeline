# Project: Youtube trending video pipeline through dbt and bigquery data warehouse
## Project summary
- This project aims to build a cloud data warehouse solution for thrending videos from the youtube api. Different stages of the tables are created using medallion architechture and final analysis of succesful channels and videos is published.
## Objectives
- Build an ETL pipeline that extracts the data hourly (through github actions workflow) from the youtube api (https://developers.google.com/youtube/v3) , send it to Bigquery and transform it further through Dbt inside the warehouse. 

System architecture:

![Architecture Exercises](https://github.com/user-attachments/assets/0953b6ce-838c-4798-9a21-20d90668f352)

## Database schema design 

- The two main tables have the following schema as example. Additionaly a star schema folder is created into a facts table and 3 dimensions tables using both these tables joined on video_id.



![Architecture Exercises(1)](https://github.com/user-attachments/assets/f67d3577-20d2-4ce4-a44d-aaa07dbfd6d0)


Dbt is used to create the different tables with sql queries and schema specifications.

<img width="961" height="947" alt="image" src="https://github.com/user-attachments/assets/d85ca339-1650-40a6-bb72-547b349805a1" />
