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


#### Data Analysis

For the data analysis I focused on insights in the following entities:
- stores
- promotions
- persons
- items

###### I. Stores Insights:
- Most profitable per year.
- Stores profits per year.
- Quantity of items type sold per store.

###### II. Promotions Insights:
- Top 5 best performing promotions.
- Top 5 worst performing promotions.

###### III. Persons
- Promotions of a given person.
- Promotions sugestions based on persons and potential friends shopping activity.
- Shopping history of a given person.
- Transfers of a given person
- Top 10 money receivers from transfers.
- Top 10 money senders by transfers.
- Potential friends of a given person.

###### IV. Items
- Top 5 selling items.


All of these insights can be accessed throug an API using the routes included in the table at the bottom of the document.


## How to run

The project was built using micro-framework Python Flask and a relational database using MySQL hosted on Azure Cloud Services. I recomend to create a virtual environment first and installing the needed python modules inside the environment. 

##### 1. Setting the virtual environment

First you must install python's virtualenv module. The command to install it is:

> pip install virtualenv

Now create the virtual environment with the following command:

> python -m venv .venv

The previous command will create a folder in the root project folder called '.venv'
To activate the environment in Mac and Linux run the command:

> source .venv/bin/activate

##### 2. Installing the required modules and running the application
Once the environment is activated install the required modules by using the following command:

> pip install -r requirements.txt

At this point the project should be ready to run. As mentioned before the database is being hosted on Azure Cloud Services and all the processed data is already in the database. Nevertheless you can reset the database and store the data by running the following command:

> python app.py --setup-database=True

This command will delete all the existing tables in the database if any, create new tables and load the data to the corresponding tables. If you dont wish to setup the database run the command without the "--set-database" argument or run instead the command:

> flask run

##### 3. Accessing the data and insights through API Calls

To access the data insights use the following table below 'API Calls Table' to refer to the desired routes. In your prefered browser insert the host followed by the port number specified by the flask application and the route. Your url should look similar to the following:

> http://127.0.0.1:5000/stores/most-profitable

In the previous url the host is 'http://127.0.0.1' and the port is '5000'.

##### API Calls Table

*Optional parameter.

|  Route   | Method | Query Params  |   Body Values  |  Description  |
| --- | --- | --- | --- | --- |
|  /stores/most-profitable                    | GET |                                |  | Returns the store most profitable per year. |
|  /stores/sellings/year                      | GET | store_id*                      |  | Returns profits per year for all stores. Results can be filtered by store using the store_id param.|
|  /stores/sellings/item                      | GET | store_id*                      |  | Returns each items quantity sells for all stores. Results can be filtered by store using the store_id param.|
|  /promotions                                | GET | promotion*, group_by_person*   |  | Returns all the promotions stored in the database. Results can be grouped by person if the optional query params are provided. |
|  /promotions/best-performing                | GET |                                |  | Returns the top 5 best performing promotions. |
| /promotions/worst-performing                | GET |                                |  | Returns the top 5 worst performing promotions. |
| /persons/promotions/\<int:person_id\>         | GET |                                |  | Returns all the promotions for a given person. |
| /persons/promotions/sugestion/\<int:promotion_id\>         | GET |                                |  | Returns other potential sugestions for promotions if any based on user and potential friends buying activity. |
| /persons/shopping-history/\<int:person_id\>   | GET |                                |  | Returns shopping history for a given person. |
| /persons/transfers                          | GET | person_id*, role*              |  | Returns all the transfers stored in the database. Results can be filtered by person and role using the query params. |
| /persons/transfers/top-money-receivers      | GET |                                |  | Returns the top 10 persons that have receive the highest total amount of money from tranfers. |
| /persons/transfers/top-money-senders        | GET |                                |  | Returns the top 10 persons that have sent the highest total amount of money by tranfers. |
| /persons/potential-friends/\<int:person_id\>  | GET |                                |  | Returns the list of potential friends based on transfers activity. |
| /items/top-sellers                          | GET |                                |  | Returns the top 5 selling items. |

