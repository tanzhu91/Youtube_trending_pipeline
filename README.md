# Project: Youtube trending video pipeline through dbt and bigquery data warehouse
## Project summary
- This project aims to take trending video data from the youtube api https://developers.google.com/youtube/v3 and transform it through dbt into the bigquery data warehouse. Different stages of the tables are created using medallion architechture and final analysis of succesful channels and videos is published.

System architecture:

<img width="960" height="540" alt="Architecture Exercises(1)" src="https://github.com/user-attachments/assets/d15bb74d-07bf-409d-99da-dfd9df475298" />

## Database schema design 

- The two main tables have the following schema as example. Additionaly a star schema folder is created into a facts table and 3 dimensions tables using both these tables.

<img width="960" height="540" alt="Architecture Exercises(2)" src="https://github.com/user-attachments/assets/d960dd83-f1ee-4643-977b-e4510284b1ff" />



Dbt is used to create the different tables with sql queries and schema specifications.

<img width="961" height="947" alt="image" src="https://github.com/user-attachments/assets/d85ca339-1650-40a6-bb72-547b349805a1" />
