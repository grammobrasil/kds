import pymongo
from flask import Blueprint, request, jsonify
from bson.json_util import dumps
from kds.config import Config

client = pymongo.MongoClient(Config.atlas_access)

api_bp = Blueprint('api', __name__)

pipe_base = [
    {
        "$lookup":
        {
            "from": "op_marcas",
            "localField": "marca_id",
            "foreignField": "_id",
            "as": "marca"
        },
    },
    {
        "$lookup":
        {
            "from": 'op_nutr',
            "localField": "cod_nutr",
            "foreignField": "_id",
            "as": "nutriente"
        },
    },
    {
        '$unwind': '$marca'
    },
    {
        '$unwind': '$marca.descr'
    },
    {
        '$unwind': '$nutriente'
    },
    {
        '$unwind': '$nutriente.nome_simples'
    },
]
pipe_proj = [
    {
        "$project":
            {
                "_id": 1,
                'cod_nutr': 1,
                'nutriente': '$nutriente.nome_simples',
                'marca_id': 1,
                'marca': '$marca.descr',
                'descr': 1,
                'un': 1,
                'g': 1,
            },
    },
]


def get_item(coll, _id):
    pipe_id = [
        {
           '$match': {'_id': _id}
        },
    ]
    return client.dev[coll].aggregate(pipe_id + pipe_base + pipe_proj)


@api_bp.route("/marcas", methods=['GET'])
def marcas():
    if request.args.get('term'):
        _filter = request.args.get('term')
        marcas = list(client.dev.op_marcas.find())
        newlist = []
        for v in marcas:
            if _filter.lower() in v["descr"].lower():
                newlist.append({
                    '_id': str(v['_id']),
                    'descr': v['descr'],
                })
        return jsonify(newlist)


@api_bp.route("/insumos", methods=['GET', 'POST'])
def insumos():
    if request.args.get('item'):
        item = get_item('op_insumos', request.args.get('item'))
        return jsonify(list(item)[0])

    if request.args.get('term'):
        lista = []

        for doc in client.dev.op_insumos.aggregate(pipe_base + pipe_proj):
            txt = "{nutriente} - {marca}, {descr}, {g} g/un, unidade : {un}"
            teste = {}
            teste = {'value': doc['_id'], 'label': txt.format(**doc)}
            lista.append(teste)

        _filter = request.args.get('term')
        newlist = []
        for v in lista:
            if _filter.lower() in v["label"].lower():
                newlist.append(v)
        return jsonify(newlist)

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
    coll = request.args.get('coll')
    if request.args.get('field'):
        field = request.args.get('field')
        value = request.args.get('value')
        item = client.dev[coll].find_one({field: value})
        return jsonify(item)

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
        return dumps(list(client.dev[coll].find()))
