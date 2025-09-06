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

After analysing the data we get top channells with videos trending within 1 day. Their category is also visible with Entertainment being a big one.

<img width="1189" height="690" alt="Channels trending channels within 1 day" src="https://github.com/user-attachments/assets/9221f574-a499-4892-8f34-34e3faa79eb2" />
Most of these channels have similar amount of total views but one of them is about 5 times more than the others.

<img width="1189" height="690" alt="Channels trending videos within 1 day and total views" src="https://github.com/user-attachments/assets/49ce711e-981d-4984-9d44-56a59ecfd69b" />

Most common categories for videos trending within one are entertainment , sports and gaming but not true for the top channels.
<img width="1189" height="690" alt="Number of trending videos within 1 day and their category" src="https://github.com/user-attachments/assets/71479120-dc47-4376-8ea7-089d59d3e5ce" />
