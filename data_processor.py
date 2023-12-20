import json
import yaml
import mysql.connector
import pandas as pd



class DataProcessor():

    def __init__(self, path):
        self.path = path
        self._process_people_data()
        self._process_promotions_data()
        self._process_transactions_data()
        self.__load_transfers_data()


    def __load_people_data(self):
        # Open files
        people_json_file = open(self.path + '/people.json')
        people_yaml_file = open(self.path + '/people.yml')

        # Load data as dicts
        people_json_data = json.load(people_json_file)['people']
        people_yaml_data = yaml.safe_load(people_yaml_file)['people']

        people_json_file.close()
        people_yaml_file.close()

        # Turn dicts into data frames
        self.people_json_df = pd.json_normalize(people_json_data)
        self.people_yaml_df = pd.json_normalize(people_yaml_data)

    def __load_transfers_data(self):

        # Load transfers data as a pandas dataframe
        self.transfers_df = pd.read_csv(self.path + '/transfers.csv')
    
    def _process_people_data(self):
        """
        This method takes the loaded data as it is from the two files with the peoples
        data, tranforms it to be identical and gets the union.
        """

        # Load people data as pandas dataframes
        self.__load_people_data()

        # Clean yaml data
        city_column = self.people_yaml_df.pop('city')
        name_column = self.people_yaml_df.pop('name')
        self.people_yaml_df[['city', 'country']] = city_column.str.split(", ", expand=True)
        self.people_yaml_df[['firstName', 'surname']] = name_column.str.split(" ", expand=True)
        self.people_yaml_df.rename(columns= {'phone' : 'telephone'}, inplace=True)

        # Clean json people data
        self.people_json_df['id'] = pd.to_numeric(self.people_json_df['id'])

        # Get the union of the two dataframes
        self.people_df = pd.concat([self.people_json_df, self.people_yaml_df], ignore_index=True)
        self.people_df.drop_duplicates(inplace=True)
        self.people_df.sort_values('id', inplace=True)
        self.people_df.reset_index(drop=True, inplace=True)

    def _process_promotions_data(self):

        """
        This method loads the promotions data as a pandas dataframe, assign the 
        person id associated with each record and remove the clien_email and 
        telephone columns.
        """

        # Load data as pandas dataframe
        self.promotions_df = pd.read_csv(self.path + '/promotions.csv')

        ids = []

        # Go over the dataframe to get the person id related to each record
        for i in range(len(self.promotions_df)):

            email = self.promotions_df.loc[i, 'client_email']

            if email != "''":
                result = self.people_df[self.people_df['email'] == email]
                ids.append(result['id'].values[0])
            
            else:
                phone_num = self.promotions_df.loc[i, 'telephone']
                result = self.people_df[self.people_df['telephone'] == phone_num]
                ids.append(result['id'].values[0])

        self.promotions_df['responded'] = self.promotions_df['responded'].map({'Yes': 1, 'No': 0})
            
        self.promotions_df['id'] = ids # assign the person id related to each record
        self.promotions_df.drop(['client_email', 'telephone'], inplace=True, axis=1) # drop columns not needed anymore

    def _process_transactions_data(self):

        """
        This method loads the transactions data as a pandas dataframe and assigns
        the person id of the buyer associated with each transaction.
        """

        # Load transactions data as a pandas dataframe
        self.transactions_df = pd.read_xml(self.path + '/transactions.xml')

        self.stores_names = set(self.transactions_df['store'].values)

        ids = []

        # Go over the dataframe to get the person id related to each record
        for i in range(len(self.transactions_df)):
            buyer_name = self.transactions_df['buyer_name'][i]
            for j in range(len(self.people_df)):
                formated_name = self.people_df['firstName'][j][0] + ". " + self.people_df['surname'][j]

                # If the buyer match a person store the id
                if buyer_name == formated_name:
                    ids.append(self.people_df['id'][j])
                    break

        self.transactions_df['person_id'] = ids # assign the person id related to each record

    def get_processed_data(self):
        """
        This method returns the processed data from the given files. 

        output:
            - people_df : pandas.DataFrame
            - promotions_df : pandas.DataFrame
            - transactions_df : pandas.DataFrame
            - transfers_df : pandas.Dataframe
            - stores_names : set()
        """
        
        return self.people_df, self.promotions_df, self.transactions_df, self.transfers_df, self.stores_names
    
