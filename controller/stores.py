from model.stores import StoresDAO
from flask import jsonify
import re


class Stores():

    def get_most_profitable_stores_per_year(self):

        return jsonify(StoresDAO().get_most_profitable_stores_per_year())
    

    def get_sellings_per_year(self, args):

        if args.get('store_id'):
            if not re.match("^\\d+$", args.get('store_id')):
                return jsonify("The value of store_id must be a number.")
            
            store_profits = StoresDAO().get_sellings_per_year_by_store(args.get('store_id'))

            if not store_profits:
                return jsonify("Store not found"), 404
            
            return jsonify(store_profits)
        
        return jsonify(StoresDAO().get_sellings_per_store_per_year())
    

    def get_top_five_selling_items(self):

        return jsonify(StoresDAO().get_top_five_selling_items())
    

    def get_items_quantity_sold_by_all_stores(self, args):

        if args.get('store_id'):

            if not re.match("^\\d+$", args.get('store_id')):
                return jsonify("The value of store_id must be a number.")
            
            item_sells = StoresDAO().get_items_quantity_sold_by_store(args.get('store_id'))

            if not item_sells:
                return jsonify("Store not found"), 404
            
            return jsonify(item_sells)
        

        return jsonify(StoresDAO().get_items_quantity_sold_by_all_stores())