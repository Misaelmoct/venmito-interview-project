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
                                 json_arrayagg(json_object('item', item, 'transaction_date', transaction_date, 'store', store_name)) AS shopping_history
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
    

    def get_all_transfers(self):
        conn = get_db_conn()
        cursor = conn.cursor(dictionary=True)
        query = """
        WITH senders_transfers AS (SELECT transfer_id, sender_id AS person_id, first_name, last_name, amount, transfer_date
                                FROM persons p INNER JOIN transfers t ON p.person_id = t.sender_id),
            recipients_transfers AS (SELECT transfer_id, recipient_id AS person_id, first_name, last_name, amount, transfer_date
                                    FROM persons p INNER JOIN transfers t ON p.person_id = t.recipient_id)


        SELECT s.transfer_id,
            JSON_OBJECT('first_name', s.first_name, 'last_name', s.last_name, 'person_id', s.person_id) AS sender,
            JSON_OBJECT('first_name', r.first_name, 'last_name', r.last_name, 'person_id', r.person_id) AS recipient,
            s.amount,
            s.transfer_date
        FROM senders_transfers s INNER JOIN recipients_transfers r ON s.transfer_id = r.transfer_id
        """

        cursor.execute(query)
        results = cursor.fetchall()
        for i in range(len(results)):
            results[i]['sender'] = json.loads(results[i]['sender'])
            results[i]['recipient'] = json.loads(results[i]['recipient'])
        
        cursor.close()
        return results
    
    def get_transfer_by_sender(self, sender_id):
        conn = get_db_conn()
        cursor = conn.cursor(dictionary=True)
        query = """
        WITH senders_transfers AS (SELECT transfer_id, sender_id AS person_id, first_name, last_name, amount, transfer_date
                                FROM persons p INNER JOIN transfers t ON p.person_id = t.sender_id),
            recipients_transfers AS (SELECT transfer_id, recipient_id AS person_id, first_name, last_name, amount, transfer_date
                                    FROM persons p INNER JOIN transfers t ON p.person_id = t.recipient_id)


        SELECT s.transfer_id,
            JSON_OBJECT('first_name', s.first_name, 'last_name', s.last_name, 'person_id', s.person_id) AS sender,
            JSON_OBJECT('first_name', r.first_name, 'last_name', r.last_name, 'person_id', r.person_id) AS recipient,
            s.amount,
            s.transfer_date
        FROM senders_transfers s INNER JOIN recipients_transfers r ON s.transfer_id = r.transfer_id
        WHERE s.person_id = %s
        """

        cursor.execute(query, (sender_id,))
        results = cursor.fetchall()
        for i in range(len(results)):
            results[i]['sender'] = json.loads(results[i]['sender'])
            results[i]['recipient'] = json.loads(results[i]['recipient'])
        cursor.close()
        return results
    
    def get_transfer_by_receiver(self, receiver_id):
        conn = get_db_conn()
        cursor = conn.cursor(dictionary=True)
        query = """
        WITH senders_transfers AS (SELECT transfer_id, sender_id AS person_id, first_name, last_name, amount, transfer_date
                                FROM persons p INNER JOIN transfers t ON p.person_id = t.sender_id),
            recipients_transfers AS (SELECT transfer_id, recipient_id AS person_id, first_name, last_name, amount, transfer_date
                                    FROM persons p INNER JOIN transfers t ON p.person_id = t.recipient_id)


        SELECT s.transfer_id,
            JSON_OBJECT('first_name', s.first_name, 'last_name', s.last_name, 'person_id', s.person_id) AS sender,
            JSON_OBJECT('first_name', r.first_name, 'last_name', r.last_name, 'person_id', r.person_id) AS recipient,
            s.amount,
            s.transfer_date
        FROM senders_transfers s INNER JOIN recipients_transfers r ON s.transfer_id = r.transfer_id
        WHERE r.person_id = %s
        """

        cursor.execute(query, (receiver_id,))
        results = cursor.fetchall()
        for i in range(len(results)):
            results[i]['sender'] = json.loads(results[i]['sender'])
            results[i]['recipient'] = json.loads(results[i]['recipient'])
        cursor.close()
        return results
    
    def get_all_transfers_by_person(self, p_id):
        conn = get_db_conn()
        cursor = conn.cursor(dictionary=True)
        query = """
        WITH senders_transfers AS (SELECT transfer_id, sender_id AS person_id, first_name, last_name, amount, transfer_date
                                FROM persons p INNER JOIN transfers t ON p.person_id = t.sender_id),
            recipients_transfers AS (SELECT transfer_id, recipient_id AS person_id, first_name, last_name, amount, transfer_date
                                    FROM persons p INNER JOIN transfers t ON p.person_id = t.recipient_id)


        SELECT s.transfer_id,
            JSON_OBJECT('first_name', s.first_name, 'last_name', s.last_name, 'person_id', s.person_id) AS sender,
            JSON_OBJECT('first_name', r.first_name, 'last_name', r.last_name, 'person_id', r.person_id) AS recipient,
            s.amount,
            s.transfer_date
        FROM senders_transfers s INNER JOIN recipients_transfers r ON s.transfer_id = r.transfer_id
        WHERE r.person_id = %s OR s.person_id = %s
        """

        cursor.execute(query, (p_id, p_id))
        results = cursor.fetchall()
        for i in range(len(results)):
            results[i]['sender'] = json.loads(results[i]['sender'])
            results[i]['recipient'] = json.loads(results[i]['recipient'])
        cursor.close()
        return results
    
    def get_top_ten_money_receivers(self):
        """
        Returns the top 10 persons that had received the highest total amount of
        money from transfers.
        """

        conn = get_db_conn()
        cursor = conn.cursor(dictionary=True)
        query = """
        SELECT p.person_id, first_name, last_name, SUM(amount) AS total
        FROM persons p INNER JOIN transfers t on p.person_id = t.recipient_id
        GROUP BY p.person_id
        ORDER BY SUM(amount) DESC
        LIMIT 10;
        """

        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        return results
    
    def get_top_ten_money_senders(self):
        """
        Returns the top 10 persons that have sent the highest total amount of
        money from transfers.
        """

        conn = get_db_conn()
        cursor = conn.cursor(dictionary=True)
        query = """
        SELECT p.person_id, first_name, last_name, SUM(amount) AS total
        FROM persons p INNER JOIN transfers t on p.person_id = t.sender_id
        GROUP BY p.person_id
        ORDER BY SUM(amount) DESC
        LIMIT 10;
        """

        cursor.execute(query)
        results = cursor.fetchall()
        cursor.close()
        return results


    def get_potential_friends(self, p_id):
        """
        This method return a list of potential friends of a given person. The data from transfers
        is used to infer which persons could be potential friends.
        """

        conn = get_db_conn()
        cursor = conn.cursor(dictionary=True)
        query = """
        WITH friends_list AS (SELECT person_id, first_name, last_name
                            FROM persons p INNER JOIN transfers t ON p.person_id = t.recipient_id
                            WHERE sender_id = %s
                            UNION
                            SELECT person_id, first_name, last_name
                            FROM persons p INNER JOIN transfers t ON p.person_id = t.sender_id
                            WHERE recipient_id = %s)

        SELECT p.person_id, p.first_name, p.last_name,
            JSON_ARRAYAGG(JSON_OBJECT('person_id', fl.person_id, 'first_name', fl.first_name, 'last_name', fl.last_name)) AS friends
        FROM persons p CROSS JOIN friends_list fl
        WHERE p.person_id = %s
        GROUP BY p.person_id;
        """

        cursor.execute(query, (p_id, p_id, p_id))
        results = cursor.fetchone()
        if results:
            results['friends'] = json.loads(results['friends'])
        cursor.close()
        return results
        
    def get_promotion_info(self, promotion_id):
        conn = get_db_conn()
        cursor = conn.cursor(dictionary=True)
        query = """
        SELECT promotion_id, person_id, promotion, responded
        FROM promotions WHERE promotion_id = %s
        AND responded = 0
        """

        cursor.execute(query, (promotion_id,))
        results = cursor.fetchone()
        cursor.close()
        return results