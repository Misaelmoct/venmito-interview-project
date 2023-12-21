from tools.database_manager import get_db_conn
import json




class StoresDAO():

    def get_most_profitable_stores_per_year(self):
        conn = get_db_conn()
        cursor = conn.cursor(dictionary=True)
        query = """
        WITH profits_per_store_and_year AS (SELECT store_name, SUM(price) AS sellings, YEAR(transaction_date) AS year FROM stores s
                                    INNER JOIN transactions t on s.store_id = t.store_id
                                    GROUP BY YEAR(transaction_date), store_name
                                    ORDER BY YEAR(transaction_date), sellings),
        most_profits_per_year AS (SELECT year, MAX(sellings) AS sellings FROM profits_per_store_and_year
                                        GROUP BY year)

        SELECT mpy.year, store_name, mpy.sellings
        FROM profits_per_store_and_year psy INNER JOIN most_profits_per_year mpy 
        ON (psy.sellings = mpy.sellings AND psy.year = mpy.year);
        """

        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        return results


    def get_sellings_per_store_per_year(self):
        conn = get_db_conn()
        cursor = conn.cursor(dictionary=True)
        query = """
        WITH year_profits AS (SELECT s.store_id, store_name, SUM(price) AS profits, YEAR(transaction_date) AS year
                            FROM stores s INNER JOIN transactions t on s.store_id = t.store_id
                            GROUP BY s.store_id, store_name, YEAR(transaction_date))

        SELECT store_id, store_name, JSON_OBJECTAGG(year, profits) AS profits
        FROM year_profits
        GROUP BY store_name, store_id
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        for i in range(len(results)):
            results[i]['profits'] = json.loads(results[i]['profits'])
        cursor.close()
        return results
    
    def get_sellings_per_year_by_store(self, store_id):
        conn = get_db_conn()
        cursor = conn.cursor(dictionary=True)
        query = """
        WITH year_profits AS (SELECT s.store_id, store_name, SUM(price) AS profits, YEAR(transaction_date) AS year
                            FROM stores s INNER JOIN transactions t on s.store_id = t.store_id
                            GROUP BY s.store_id, store_name, YEAR(transaction_date))

        SELECT store_id, store_name, JSON_OBJECTAGG(year, profits) AS profits
        FROM year_profits
        WHERE store_id = %s
        GROUP BY store_name, store_id;
        """
        
        cursor.execute(query, (store_id,))
        results = cursor.fetchone()
        if results:
            results['profits'] = json.loads(results['profits'])
        cursor.close()
        return results
    
    def get_top_five_selling_items(self):
        conn = get_db_conn()
        cursor = conn.cursor(dictionary=True)
        query = """
        SELECT item, COUNT(item) AS 'quantity sold' FROM transactions
        GROUP BY item
        ORDER BY COUNT(item) DESC
        LIMIT 5;
        """

        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        return results
    

    def get_items_quantity_sold_by_all_stores(self):
        conn = get_db_conn()
        cursor = conn.cursor(dictionary=True)
        query = """
        WITH stores_sells AS (SELECT s.store_id, store_name, item, COUNT(item) AS quantity_sold
                            FROM transactions INNER JOIN stores s ON transactions.store_id = s.store_id
                            GROUP BY item, store_name, s.store_id)

        SELECT store_id, store_name, JSON_OBJECTAGG(item, quantity_sold) AS items_sells
        FROM stores_sells
        GROUP BY store_name, store_id
        """

        cursor.execute(query)
        results = cursor.fetchall()
        for i in range(len(results)):
            results[i]['items_sells'] = json.loads(results[i]['items_sells'])
        cursor.close()
        return results
    
    def get_items_quantity_sold_by_store(self, store_id):
        conn = get_db_conn()
        cursor = conn.cursor(dictionary=True)
        query = """
        WITH stores_sells AS (SELECT s.store_id, store_name, item, COUNT(item) AS quantity_sold
                            FROM transactions INNER JOIN stores s ON transactions.store_id = s.store_id
                            GROUP BY item, store_name, s.store_id)

        SELECT store_id, store_name, JSON_OBJECTAGG(item, quantity_sold) AS items_sells
        FROM stores_sells
        WHERE store_id = %s
        GROUP BY store_name, store_id
        """

        cursor.execute(query, (store_id,))
        results = cursor.fetchone()
        if results:
            results['items_sells'] = json.loads(results['items_sells'])
        cursor.close()
        return results
