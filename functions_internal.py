import pymongo
from bson.json_util import dumps
from instaloader import Instaloader, Profile
from kds.config import Config

client = pymongo.MongoClient(Config.atlas_access)


def compra_lookup(bubble_id):
    out = client.grammo.compras.aggregate([
        {
            '$match':
            {
                'bubble._id': bubble_id
            },
        },
        {
            "$lookup":
            {
                "from": "usr",
                "localField": "dados.usr",
                "foreignField": "_id",
                "as": "usr"
            },
        },
        {
            '$unwind': '$pedidos'
        },
        {
          '$lookup': {
            'from': 'usr',
            'let': {'endereço_id': '$pedidos.endereço._id'},
            'pipeline': [

                {
                    '$unwind': '$endereços'
                },

                {
                    '$match':
                        {
                            '$expr':
                            {
                               '$eq': ['$$endereço_id', '$endereços._id']
                            }
                        }
                    }
              ],
            'as': 'pedidos.end'
            }
        },
        {
            '$unwind': '$pedidos.itens'
        },
        {
            '$unwind': '$pedidos.itens.conteúdo'
        },
        {
            "$lookup":
            {
                "from": "produtos",
                "localField": "pedidos.itens.conteúdo.produto",
                "foreignField": "_id",
                "as": "produto_info"
            },
        },

        {
            '$addFields': {
                'conteúdo': {
                    '$map': {
                        'input': "$produto_info",
                        'in': {
                            '$mergeObjects': [
                                "$$this",
                                '$pedidos.itens.conteúdo'
                            ]},
                    },
                },
            },
        },
        {
            "$group":
            {
                "_id": {
                    "pedido": "$pedidos.num",
                    "item": "$pedidos.itens.num",
                },
                'conteúdo_item': {
                    "$addToSet": '$conteúdo',
                },
                'item_num': {
                    "$first": '$pedidos.itens.num',
                },
                'dados': {
                    "$first": '$dados',
                },
                'end': {
                    "$first": '$pedidos.end',
                },
                'usr': {
                    "$first": '$usr',
                },
                'bubble_pedido': {
                    "$first": '$pedidos.bubble',
                },
                'entrega_hora': {
                     "$first": '$pedidos.entrega_hora'
                },
            }
        },
        {
            "$group":
            {
                "_id": "$_id.pedido",
                "conteúdo_pedido":
                {
                    "$addToSet": '$$ROOT'
                },
            }
        },
        {
            "$sort":
            {
                "conteúdo_pedido._id.pedido": 1,
                "conteúdo_pedido._id.item": 1
            }
        },
        {
            "$project":
                {
                    "conteúdo_pedido.usr.endereços": 0
                },
        }

    ])
    return list(out)


def mongo_updated_times(coll_list):

    updated_time_dic = {}
    for item in coll_list:
        pipe = [
            {
                '$addFields': {
                    "mongotime": {
                        "$toDate": "$_id"
                    }
                }
            },
            {
                "$sort":
                    {"mongotime": -1}
            },
            {
                "$limit": 1
            },
        ]

        db = client.grammo[item]
        lastdoc = list(db.aggregate(pipe))
        doc_time = lastdoc[0]['mongotime']
        updated_time_dic.update({item: doc_time.isoformat()})

    json_file = 'mongo_updated_times.json'

    with open(json_file, 'w', encoding='utf-8') as file:
        try:
            json_data = dumps(updated_time_dic)
            file.write(json_data)
        except Exception as e:
            print(e)


def mongo_updated_time(coll):

    pipe = [
        {
            '$addFields': {
                "mongotime": {
                    "$toDate": "$_id"
                }
            }
        },
        {
            "$sort":
                {"mongotime": -1}
        },
        {
            "$limit": 1
        },
    ]

    db = client.grammo[coll]
    lastdoc = list(db.aggregate(pipe))
    return lastdoc[0]['mongotime']


def insta_engagement_rate(target_profile):
    loader = Instaloader()
    loader.load_session_from_file('r.thiesen')

    profile = Profile.from_username(loader.context, target_profile)
    data = {}

    num_followers = profile.followers
    total_num_likes = 0
    total_num_comments = 0
    total_num_posts = 0

    for post in profile.get_posts():
        total_num_likes += post.likes
        total_num_comments += post.comments
        total_num_posts += 1

    data['num_followers'] = num_followers
    data['total_num_likes'] = total_num_likes
    data['total_num_comments'] = total_num_comments
    data['total_num_posts'] = total_num_posts

    engagement = float(
        total_num_likes + (total_num_comments * 1.5)
        ) / (num_followers * total_num_posts)
    data['engagement'] = engagement * 100

    return data


def find_client(regex):
    tmp = client.grammo.usr.aggregate([
        {
            '$match': {
                'bio.nome': {
                    '$regex': regex
                    }
            }
        }
    ])

    return list(tmp)
