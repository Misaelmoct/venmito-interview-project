from flask import Flask, request, jsonify, g, render_template
from flask_cors import CORS
from controller.stores import Stores
from controller.persons import Persons
from tools.database_manager import DatabaseManager as DBM
from tools.data_processor import DataProcessor as DP
import sys
import re


app = Flask(__name__)
CORS(app)

@app.teardown_appcontext
def teardown_db(exception):
   db = g.pop('db', None)

   if db is not None:
      db.close()


@app.route("/")
def home():
   return jsonify("Venmito Data Analytics API")


@app.route("/stores/most-profitable", methods=['GET'])
def get_most_profitable():
   if request.method == 'GET':
      return Stores().get_most_profitable_stores_per_year()
   
   else:
      return jsonify("Method not allowed"), 405
   
@app.route("/stores/sellings/year", methods=['GET'])
def get_sellings():
   if request.method == 'GET':
      return Stores().get_sellings_per_year(request.args)
   
   else:
      return jsonify("Method not allowed"), 405
   
@app.route("/stores/sellings/item", methods=['GET'])
def get_item_sellings():
   if request.method == 'GET':
      return Stores().get_items_quantity_sold_by_all_stores(request.args)
   
   else:
      return jsonify("Method not allowed."), 405

@app.route("/promotions", methods=['GET'])
def get_promotions():
   if request.method == 'GET':
      return Persons().get_promotions_by_promotion_type(request.args)
   
   else:
      return jsonify("Method not allowed"), 405

@app.route("/promotions/best-performing", methods=['GET'])
def get_best_performers():
   if request.method == 'GET':
      return Persons().get_best_performing_promotions()

   else:
      return jsonify("Method not allowed"), 405

@app.route("/promotions/worst-performing", methods=['GET'])
def get_worst_performers():
   if request.method == 'GET':
      return Persons().get_worst_performing_promotions()

   else:
      return jsonify("Method not allowed"), 405   

@app.route("/persons/promotions/<int:person_id>", methods=['GET'])
def get_person_promotions(person_id):
   if request.method == 'GET':
      return Persons().get_promotions_by_person_id(person_id)
   
   else:
      return jsonify("Method not allowed"), 405
   
@app.route("/persons/shopping-history/<int:person_id>", methods=['GET'])
def get_shopping_history(person_id):
   if request.method == 'GET':
      return Persons().get_shopping_history(person_id)
   
   else:
      return jsonify("Method not allowed"), 405
   
@app.route("/items/top-sellers", methods=['GET'])
def get_top_selling_items():
   if request.method == 'GET':
      return Stores().get_top_five_selling_items()
   
   else:
      return jsonify("Method not allowed"), 405
   
@app.route("/transfers", methods=['GET'])
def get_all_transfers():
   if request.method == 'GET':
      return Persons().get_all_transfers()
   
   else:
      return jsonify("Method not allowed"), 405



if __name__ == "__main__":

    # Verify if the argument --setup-database was provided
    if len(sys.argv) > 1 and re.match("--setup-database=(True$|False$)", sys.argv[1]):
       
       # If value provided is True then setup database
        if re.match("True", sys.argv[1].split("=", 1)[1]):
            dp = DP('data')
            people_df, promotions_df, transactions_df, transfers_df, stores_names = dp.get_processed_data()
            dbm = DBM(people_df, promotions_df, transactions_df, transfers_df, stores_names)
            dbm.save_data()

    app.run()

      