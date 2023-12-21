from tools.database_manager import get_db_conn
import json




class PersonsDAO():

    def get_all_promotions_grouped_by_person(self):
        conn = get_db_conn()
        cursor = conn.cursor(dictionary=True)
        query = """
        SELECT p.person_id, first_name, last_name, json_arrayagg(json_object('promotion', promotion, 'responded', responded)) AS promotions
        FROM persons p INNER JOIN promotions pm ON p.person_id = pm.person_id
        GROUP BY p.person_id;
        """

        cursor.execute(query)
        results = cursor.fetchall()
        for i in range(len(results)):
            results[i]['promotions'] = json.loads(results[i]['promotions'])

        cursor.close()
        return results
    
    def get_all_promotions(self):
        conn = get_db_conn()
        cursor = conn.cursor(dictionary=True)
        query = """
        SELECT p.person_id, first_name, last_name, promotion, responded
        FROM persons p INNER JOIN promotions pm ON p.person_id = pm.person_id
        """

        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        return results


    def get_promotion_by_promotion_type(self, promotion):
        conn = get_db_conn()
        cursor = conn.cursor(dictionary=True)
        query = """
        SELECT p.person_id, first_name, last_name, promotion, responded
        FROM persons p INNER JOIN promotions pm ON p.person_id = pm.person_id
        WHERE promotion = %s
        """

        cursor.execute(query, (promotion,))
        results = cursor.fetchall()
        cursor.close()
        return results
    
    def get_promotion_by_person_id(self, p_id):
        conn = get_db_conn()
        cursor = conn.cursor(dictionary=True)
        query = """
        SELECT p.person_id, first_name, last_name, json_arrayagg(json_object('promotion', promotion, 'responded', responded)) AS promotions
        FROM persons p INNER JOIN promotions pm ON p.person_id = pm.person_id
        WHERE p.person_id = %s
        GROUP BY p.person_id;
        """

        cursor.execute(query, (p_id,))
        results = cursor.fetchone()
        results['promotions'] = json.loads(results['promotions']) # returned values is a string and must be converted into json
        cursor.close()
        return results
    
    def get_all_tranfers(self):
        conn = get_db_conn()
        cursor = conn.cursor(dictionary=True)
        query = """
        WITH sender_info AS (SELECT p.person_id, first_name, last_name, amount, transfer_id, transfer_date
                            FROM persons p INNER JOIN transfers t ON p.person_id = t.sender_id),

            recipient_info AS (SELECT p.person_id, first_name, last_name, amount, transfer_id
                            FROM persons p INNER JOIN transfers t ON p.person_id = t.recipient_id)

        SELECT s.transfer_id, s.first_name AS sender_first_name, s.last_name AS sender_last_name,
            r.first_name AS recipient_first_name, r.last_name AS recipient_last_name, s.amount, transfer_date
        FROM sender_info s INNER JOIN recipient_info r ON s.transfer_id = r.transfer_id
        ORDER BY transfer_date
        """

        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        return results
    
    def get_promotions_count(self):
        """
        This method returns the responded and not responded count by promotions type.
        """

        conn = get_db_conn()
        cursor = conn.cursor(dictionary=True)
        query = """
        WITH responded_promotions AS (SELECT promotion, COUNT(promotion) AS count
                                    FROM promotions
                                    WHERE responded = TRUE
                                    GROUP BY promotion),
            not_responded_promotions AS (SELECT promotion, COUNT(promotion) AS count
                                    FROM promotions
                                    WHERE responded = FALSE
                                    GROUP BY promotion)

        SELECT IFNULL(rp.promotion, nrp.promotion) AS promotion, 
            IF(rp.count > 0, rp.count, 0) AS responded_count, 
            IF(nrp.count > 0, nrp.count, 0) AS not_responded_count
        FROM responded_promotions rp RIGHT OUTER JOIN not_responded_promotions nrp ON rp.promotion = nrp.promotion
        ORDER BY responded_count DESC;
        """

        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        return results
    

    def get_shopping_history(self, p_id):
        conn = get_db_conn()
        cursor = conn.cursor(dictionary=True)
        query = """
        WITH shopping_history AS (SELECT t.person_id,
                                 json_arrayagg(json_object('item', item, 'transactio_date', transaction_date, 'store', store_name)) AS shopping_history
                          FROM transactions t INNER JOIN stores s ON s.store_id = t.store_id
                          WHERE person_id = %s
                          GROUP BY t.person_id)


        SELECT p.person_id, first_name, last_name,
            IFNULL(shopping_history, json_array()) AS shopping_history
        FROM persons p LEFT OUTER JOIN shopping_history sh ON p.person_id = sh.person_id
        WHERE p.person_id = %s;
        """

        cursor.execute(query, (p_id, p_id))
        results = cursor.fetchone()
        results['shopping_history'] = json.loads(results['shopping_history'])
        cursor.close()
        return results