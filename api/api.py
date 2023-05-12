import pymongo
from flask import Blueprint, request, jsonify
from bson import json_util
import json
from kds.config import Config

client = pymongo.MongoClient(Config.atlas_access)

api_bp = Blueprint('api', __name__)

def pipe_from_dict(pipe):

    pipe_out = []
    proj_dic = {}

    # Prematch fields
    if 'prematch' in pipe:
        pipe_out.append(
            {
                '$match': pipe['prematch']
            }
        )

    # Lookup fields
    if 'lookup' in pipe:
        for field in pipe['lookup']:
            foreignField = field['foreignField'] if 'foreignField' in field else '_id' #noqa
            pipe_out.append({
                "$lookup":
                {
                    "from": field['from'],
                    "localField": field['localField'],
                    "foreignField": foreignField,
                    "as": field['as']
                },
            })
            pipe_out.append({
                '$unwind': '$' + field['as']
            })
            pipe_out.append({
                '$unwind': '$' + field['as'] + '.' + field['as_field']
            })
            proj_dic.update({field['as']: '$'+field['as']+'.'+field['as_field']})

    # Project fields
    if 'proj_fields' in pipe:
        for field in pipe['proj_fields']:
            proj_dic.update({field: 1})

        pipe_out.append({
            "$project": proj_dic
        })

    # Match fields
    match_list = []
    if 'match' in pipe:
        for field in pipe['match']:
            match_list.append(
                {field: {'$regex': pipe['match'][field], '$options': 'i'}}
                )
    
    # Term fields
    elif 'term' in pipe:
        for field in proj_dic:
            match_list.append({
                field: {'$regex': pipe['term'], '$options': 'i'}
            })

    # Add to match list
    if ('term' or 'match') in pipe:
        pipe_out.append(
            {
                '$match': 
                    {
                        "$or": match_list
                    },
            },
        )
    return pipe_out


pipe_insumos_dict = {
        'lookup': [
            {
                'from': "op_marcas",
                'localField': "marca_id",
                'as': "marca",
                'as_field': "descr"
            },
            {
                'from': "op_nutr",
                'localField': "cod_nutr",
                'as': "nutriente",
                'as_field': "nome_simples"
            },
        ],
        'proj_fields': [
            'cod_nutr', 'marca_id', 'descr', 'un', 'g'
        ],
    }


@api_bp.route("/insumos", methods=['GET', 'POST'])
def insumos():

    if request.args.get('term'):
        pipe_insumos_dict.update({'term': request.args.get('term')})
        out = []
        for doc in client.dev.op_insumos.aggregate(pipe_from_dict(pipe_insumos_dict)):
            txt = "{nutriente} - {marca}, {descr}, {g} g/un, unidade : {un}"
            item = {'value': doc['_id'], 'label': txt.format(**doc)}
            out.append(item)
        return jsonify(out)

    if request.method == 'POST':
        _json = request.json
        try:
            client.dev['op_insumos'].update_one(
                {'_id': _json['_id']},
                {"$set": _json},
                upsert=True)
            resp = jsonify('Data updated successfully!')
            resp.status_code = 200
            return resp
        except Exception as e:
            return e


@api_bp.route("/any", methods=['GET', 'POST', 'PUT'])
def any():
    if request.method == 'GET':
        pipe = {}
        db = request.args.get('db')
        coll = request.args.get('coll')

        if request.args.get('pipe'):
            pipe.update(globals()[request.args.get('pipe')])

        if request.args.get('field') and request.args.get('value'):
            field = request.args.get('field')
            value = request.args.get('value')
            pipe.update({'match': {field: value}})
        
        pipe_final = pipe_from_dict(pipe)
        mongo_out = list(client[db][coll].aggregate(pipe_final))
        page_sanitized = json.loads(json_util.dumps(mongo_out))
        return jsonify(page_sanitized)

    if request.method == 'POST':
        try:
            _json = request.json
            client.dev[coll].insert_one(_json)
            resp = jsonify('Data added successfully!')
            resp.status_code = 200
            return resp
        except Exception as e:
            return e

    else:
        return dumps(list(client.dev[coll].find({})))