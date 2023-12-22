from model.persons import PersonsDAO
from flask import jsonify
import json
import re
import datetime




class Persons():

    def _get_promotion_performing_stats(self):
        results = PersonsDAO().get_promotions_count()

        promotionPerformance = []
        for p in results:
            total_promotions = p['responded_count'] + p['not_responded_count']
            responded_percentage = round(p['responded_count'] / total_promotions, 2)
            tempDict = {
                "promotion" : p['promotion'],
                "responded_percentage" : responded_percentage,
                "total_promotions" : total_promotions
            }
            promotionPerformance.append(tempDict)
        
        return promotionPerformance


    def get_promotions_by_promotion_type(self, args):
        
        # If no promotion type is specified return all
        if not args.get('promotion'):

            # If specified return promotions grouped by person
            group_by_person = args.get('group_by_person')

        
            if group_by_person and not re.match("true$|True$|false$|False$|1$|0$", group_by_person):
                return jsonify("Wrong value provided in group_by_person query param. Must be a boolean value."), 400
            
            if re.match("true|True|1", group_by_person):
                return jsonify(PersonsDAO().get_all_promotions_grouped_by_person())

            else:
                return jsonify(PersonsDAO().get_all_promotions())
        
        return jsonify(PersonsDAO().get_promotion_by_promotion_type(args.get('promotion')))
            

    
    def get_promotions_by_person_id(self, person_id):

        
        promotions = PersonsDAO().get_promotion_by_person_id(person_id)

        responded_count = 0
        for p in promotions['promotions']:
            if p['responded']:
                responded_count += 1

        responded_percentage = round(responded_count / len(promotions), 2)

        promotions['responded_percentage'] = responded_percentage
        promotions['total_promotions'] = len(promotions['promotions'])

        return jsonify(promotions)
    

    def get_best_performing_promotions(self):
        """
        This method returns the top five performing promotions. The best performing 
        promotions are those that have the highest percentage of responded promotions.
        """

        promotionPerformance = self._get_promotion_performing_stats()
        
        # Sort list dict by responded percentage in descending order
        promotionPerformance = sorted(promotionPerformance, key=lambda d: d['responded_percentage'], reverse=True)

        return jsonify(promotionPerformance[:5])
    
    def get_worst_performing_promotions(self):

        promotionPerformance  = self._get_promotion_performing_stats()

        # Sort list dict by responded percentage in ascending order
        promotionPerformance = sorted(promotionPerformance, key=lambda d: d['responded_percentage'])

        return jsonify(promotionPerformance[:5])


    def get_shopping_history(self, p_id):

        return jsonify(PersonsDAO().get_shopping_history(p_id))
    
    def get_transfers(self, args):

        if args.get('person_id'):

            # Verify sanity of person_id value
            if not re.match("^\\d+$", args.get('person_id')):
                return jsonify("The value of person_id must be a number.")
            
            # Filter by role
            if args.get('role'):
                
                # Verify sanity of role value
                if args.get('role') not in ['sender', 'recipient']:
                    return jsonify("The value of role must be either 'sender' or 'recipient'.")
                
                # Verify if filter by sender or recipient
                if args.get('role') == 'sender':
                    transfers = PersonsDAO().get_transfer_by_sender(args.get('person_id'))
                
                else:
                    transfers = PersonsDAO().get_transfer_by_receiver(args.get('person_id'))
                
                
            else:
                transfers = PersonsDAO().get_all_transfers_by_person(args.get('person_id'))

        else:
            transfers = PersonsDAO().get_all_transfers()


        if not transfers:
                    return jsonify("No transfers found.")
        
        return jsonify(transfers)
    

    def get_top_ten_money_receivers(self):

        return jsonify(PersonsDAO().get_top_ten_money_receivers())
    
    def get_top_ten_money_senders(self):

        return jsonify(PersonsDAO().get_top_ten_money_senders())
    
    def get_potential_friends_by_person(self, person_id):

        friends_list = PersonsDAO().get_potential_friends(person_id)
        if not friends_list:
            return jsonify("No potential friends found.")
        
        return jsonify(friends_list)
    

    def get_promotion_sugestion(self, promotion_id):

        promotion_info = PersonsDAO().get_promotion_info(promotion_id)
        if not promotion_info:
            return jsonify("Promotion does not exist or is responded as 'Yes'")
        
        shopping_history = PersonsDAO().get_shopping_history(promotion_info['person_id'])
        most_recent_bought_item = {
                "item" : 'None',
                'date' : 'None'
        }
        # If user has shopping history suggest the most bought item by user
        if shopping_history['shopping_history']:

            today = datetime.datetime.today()
            items = {}
            most_recent_bought_item['date'] = datetime.datetime.strptime("1980-01-01", "%Y-%m-%d")
            
            # Get count of items bought by item type
            for transaction in shopping_history['shopping_history']:
                temp_item = transaction['item']

                # If item bought is the same as the promotion ignore it
                if temp_item == promotion_info['promotion']:
                    continue

                # If item does not already in dict store it
                if temp_item not in items:
                    items[temp_item] = 1
                else:
                    items[temp_item] += 1
                
                # Get most recent bought item
                transaction_date = datetime.datetime.strptime(transaction['transaction_date'], "%Y-%m-%d")
                if today - transaction_date < today - most_recent_bought_item['date']:
                    most_recent_bought_item['item'] = transaction['item']
                    most_recent_bought_item['date'] = transaction['transaction_date']
            
            most_bought_item = max(items, key=items.get)
            most_bought_item_quantity = items[most_bought_item]

        else:
            most_bought_item = 'None'
            most_bought_item_quantity = 'None'

        
        # If user has potential friends get the most bought item by the potential friends
        friends_list = PersonsDAO().get_potential_friends(promotion_info['person_id'])
        friends_items = {}
        if friends_list:
            for friend in friends_list['friends']:
                shopping_history = PersonsDAO().get_shopping_history(friend['person_id'])

                if not shopping_history['shopping_history']:
                    continue

                # Get count of items bought by item type
                for transaction in shopping_history['shopping_history']:
                    temp_item = transaction['item']

                    # If item bought is the same as the promotion ignore it
                    if temp_item == promotion_info['promotion']:
                        continue

                    # If item does not already in dict store it
                    if temp_item not in friends_items:
                        friends_items[temp_item] = 1
                    else:
                        friends_items[temp_item] += 1
                    
        if not friends_items:
            friends_most_bought_item = 'None'
        
        else:
            friends_most_bought_item = max(friends_items, key=friends_items.get)


        sugestion = {
            "most_bought_item": most_bought_item,
            "most_bought_item_quantity" : most_bought_item_quantity,
            "most_recent_bought_item" : most_recent_bought_item['item'],
            "most_recent_bought_item_date" : most_recent_bought_item['date'],
            "friends_most_bought_item" : friends_most_bought_item
        }

        return jsonify(sugestion)



