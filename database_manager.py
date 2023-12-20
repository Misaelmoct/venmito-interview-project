import mysql.connector
from mysql.connector import errorcode
from data_processor import DataProcessor as DP
from threading import Thread
import logging

logging.basicConfig(format="%(name)s : %(levelname)s : %(message)s")
logger = logging.getLogger("Database-Manager")
logger.setLevel(logging.INFO)

config = {
    'host' : 'venmito-database.mysql.database.azure.com',
    'user' : 'venmitoAdmin',
    'password' : 'Xtillion2023',
    'database' : 'venmito'
}

def connect_db():
   try:
      conn = mysql.connector.connect(**config)
   except mysql.connector.Error as err:
      if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
         print("Something is wrong with your user name or password")
      elif err.errno == errorcode.ER_BAD_DB_ERROR:
         print("Database does not exist")
      else:
         print(err)
   else:
      return conn
   

class DatabaseManager():

    def __init__(self, people_df, promotions_df, transactions_df, transfers_df, stores_names):
        self.people_df = people_df
        self.promotions_df = promotions_df
        self.transactions_df = transactions_df
        self.transfers_df = transfers_df
        self.stores_names = stores_names
        
    def _save_people(self, conn):
        cursor = conn.cursor(dictionary=True)
        query = """
        INSERT INTO persons (person_id, first_name, last_name, email_address, telephone, city, country)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        for i in range(len(self.people_df)):
            row = self.people_df.iloc[i]
            cursor.execute(query, (int(row['id']), row['firstName'], row['surname'], row['email'], row['telephone'], row['city'], row['country']))

        conn.commit()
        cursor.close()

    def _save_promotions(self, conn):
        cursor = conn.cursor(dictionary=True)
        query = """
        INSERT INTO promotions (person_id, promotion, responded)
        VALUES (%s, %s, %s)
        """

        for i in range(len(self.promotions_df)):
            row = self.promotions_df.iloc[i]
            cursor.execute(query, (int(row['id']), row['promotion'], int(row['responded'])))
        
        conn.commit()
        cursor.close()

    def _save_transfers(self, conn):
        cursor = conn.cursor(dictionary=True)
        query = """
        INSERT INTO transfers (sender_id, recipient_id, amount, transfer_date)
        VALUES (%s, %s, %s, %s)
        """

        for i in range(len(self.transfers_df)):
            row = self.transfers_df.iloc[i]
            cursor.execute(query, (int(row['sender_id']), int(row['recipient_id']), int(row['amount']), row['date']))

        conn.commit()
        cursor.close()
        
    def _save_stores(self, conn):
        cursor = conn.cursor(dictionary=True)
        query = """
        INSERT INTO stores (store_name)
        VALUES (%s)
        """

        for store in self.stores_names:
            cursor.execute(query, (store,))

        conn.commit()
        cursor.close()

    def _save_transactions(self, conn):
        cursor = conn.cursor(dictionary=True)

        stores_query = "SELECT * FROM stores"
        cursor.execute(stores_query)
        stores_info = cursor.fetchall()

        query = """
        INSERT INTO transactions (transaction_id, person_id, store_id, item, price, transaction_date)
        VALUES (%s, %s, %s, %s, %s, %s)
        """

        for i in range(len(self.transactions_df)):
            row = self.transactions_df.iloc[i]
            store_id = 0
            for store in stores_info:
                if store['store_name'] == row['store']:
                    store_id = store['store_id']
                    break

            cursor.execute(query, (int(row['id']), int(row['person_id']), store_id, row['item'], float(row['price']), row['transactionDate']))

        conn.commit()
        cursor.close()

    def _setup_database(self):
        logger.info("Setting up database tables wait a momment please...")
        conn = connect_db()
        cursor = conn.cursor(dictionary=True)
        
        reset_query = """
        DROP TABLE promotions, transactions, transfers, stores, persons
        """

        try: 
            cursor.execute(reset_query)

        except Exception as ex:
            # If tables are already deleted ignore exception
            if "Unknown table" in ex.msg:
                pass
            else:
                raise ex



        with open('create_tables.sql', 'r') as queries:
            percentage_completion = 20
            cur_iter = cursor.execute(queries.read(), multi=True)
            for r in cur_iter:
                logger.info("Creating tables...%s%%", percentage_completion)
                percentage_completion += 20

            conn.commit()

        logger.info("Tables successfully created.")
        cursor.close()
        conn.close()

    def save_data(self):
        # Delete old tables and create new tables
        self._setup_database()

        logger.info("Loading the data to the database wait a momment please...")
        conn1 = connect_db()
        conn2 = connect_db()
        conn3 = connect_db()

        # Start independent threads
        t1 = Thread(target=self._save_people, args=(conn1,))
        t2 = Thread(target=self._save_stores, args=(conn2,))
        t1.start()
        t2.start()

        t2.join()
        logger.info("Loading data...20%")
        t1.join()
        logger.info("Loading data...40%")

        # Start remaining dependent threads
        t1 = Thread(target=self._save_transactions, args=(conn1,))
        t2 = Thread(target=self._save_promotions, args=(conn2,))
        t3 = Thread(target=self._save_transfers, args=(conn3,))
        t1.start()
        t2.start()
        t3.start()

        t1.join()
        logger.info("Loading data...60%")
        t2.join()
        logger.info("Loading data...80%")
        t3.join()
        logger.info("Loading data...100%")

        # Save changes
        conn1.commit()
        conn2.commit()
        conn3.commit()
        
        # Close connections
        conn1.close()
        conn2.close()
        conn3.close()
        logger.info("Data sucessfully loaded.")

