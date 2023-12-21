from model.persons import PersonsDAO
from flask import jsonify
import json
import re




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
    
    def get_all_transfers(self):

        return jsonify(PersonsDAO().get_all_tranfers())
    

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