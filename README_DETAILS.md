# Venmito Data Engineering Project


## Approach

#### Data Ingestion, Matching and Conforming

To load the data I decided to use Pandas DataFrames since it makes easier handling and tranformation of the data. After the 
data was loaded I make some transformation which were made over three of the four main data sources:

- People
- Promotions
- Transactions

##### 1. People Data

The process of matching and conforming the people data involved transforming it to be identical. The data from the yaml file had the full name in a single key-value pair which was different from the json file which had two separate key-value pairs for the first and last names. I decided to split the full name into two variables named firstName and surname. The city and country were other values stored in a single key-value pair inside the yaml file which I ended splitting to be identical to the json file. Lastly, I changed the data type of the 'id' key inside the json file from string to integer. Once the data from the two sources had the same structure, I proceeded to find the union and with this I finished with the processing of the data.


##### 2. Promotions

The process of matching and conforming the promotions data was simpler since it only involved assigning the person id associated with each record from the file. Additional to that I decided to remove both the email address and the phone number from the data since the person id is enough to access these values later. Lastly, I decided to change the 'Yes' and 'No' values to 0's and 1's.

##### 3. Transactions

The process of matching the transactions data only included assigning the person id to each of the records from the file.


The data that was loaded was subsequently uploaded to a cloud-hosted MySQL database. This decision was made in order to utilize the SQL language at a later stage.