# Project: Youtube trending video pipeline through dbt and bigquery data warehouse
## Project summary
- This project aims to build a cloud data warehouse solution for thrending videos from the youtube api. Different stages of the tables are created using medallion architechture and final analysis of succesful channels and videos is published.
## Objectives
- Build an ETL pipeline that extracts the data hourly (through github actions workflow) from the youtube api (https://developers.google.com/youtube/v3) , send it to Bigquery and transform it further through Dbt inside the warehouse. 

System architecture:

![Architecture Exercises(2)](https://github.com/user-attachments/assets/86140cc2-b62c-4593-81ce-1d2920d7f89c)

## Database schema design 

- The two main tables have the following schema as example. Additionaly a star schema folder is created into a third facts table and 3 dimensions tables using both these tables joined on video_id.

![Architecture Exercises(1)](https://github.com/user-attachments/assets/f67d3577-20d2-4ce4-a44d-aaa07dbfd6d0)


Dbt is used to create the different stages of the tables with sql queries and schema specifications.

<img width="961" height="947" alt="image" src="https://github.com/user-attachments/assets/d85ca339-1650-40a6-bb72-547b349805a1" />


If we look at which hour of the day most videos are published we get an interesting pattern which makes sense , since humans are awake at specific times and consume media at specific times.

<img width="876" height="450" alt="1" src="https://github.com/user-attachments/assets/62c0c8e7-ab26-4dd0-b193-4ae4c3c2ab1d" />

The weekend seems to be the time when larger amount of views is registered compared to other days of the week.

<img width="876" height="450" alt="2" src="https://github.com/user-attachments/assets/7c4a18dc-d065-4894-9fc5-14fe7592d367" />

Certain countries seem to dominate the amount of views. It is expected since these are either in international languages or countries with a lot of people.

<img width="1017" height="450" alt="5" src="https://github.com/user-attachments/assets/0b632027-8aad-4593-b9d2-d5ecbf06b916" />

After analysing top channells with videos trending within 1 day we get their category with Entertainment the biggest one.

<img width="1200" height="700" alt="7" src="https://github.com/user-attachments/assets/0df05960-1dc1-4ad9-b125-37b219f8eef1" />

Most common categories for videos are entertainment , sports and gaming but not true for the top channel.

<img width="1017" height="450" alt="8" src="https://github.com/user-attachments/assets/33ec011b-4a8f-4131-ab57-b419a4cd83bf" />

