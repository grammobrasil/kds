from flask import Blueprint, render_template, abort
import pymongo
from bson.json_util import dumps

api_page = Blueprint('api_page', __name__,
                        template_folder='templates')

client = pymongo.MongoClient()

@api_page.route('/add', methods=['POST'])

