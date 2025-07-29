# Project: Youtube trending video pipeline through dbt and bigquery data warehouse
## Project summary
- This project aims to take trending video data from the youtube api https://developers.google.com/youtube/v3 and transform the data through dbt into the bigquery data warehouse. Different stages of the tables are created using medallion architechture and final analysis of succesful channels and videos is published.

System architecture:

<img width="770" height="430" alt="Architecture Exercises" src="https://github.com/user-attachments/assets/6c6bb426-6401-45f4-bb7f-3237c3ab6c93" />

## Database schema design 

- The two main tables have the following schema as example. Additionaly a star schema folder is created into a facts table and 3 dimensions tables using both these tables.

<img width="377" height="303" alt="Screenshot (155)" src="https://github.com/user-attachments/assets/661aa972-94e5-4559-b85b-ac268528d4b6" /> <img width="377" height="303" alt="Screenshot (154)" src="https://github.com/user-attachments/assets/23ca0b8f-e2db-4831-9aa8-c2af819fde5e" />


