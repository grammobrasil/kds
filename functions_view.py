import pymongo
import pandas as pd
from datetime import datetime, timedelta
from bson.objectid import ObjectId


from kds.config import Config
client = pymongo.MongoClient(Config.atlas_access)


# Function to get all values from distinct nutrients columns
# in 'pedidosbox' and create a new table
# with one merges column 'produto',
# based on 'nutrientes' passed.
# This is necessary because 'pedidobox' have a column for each product,

def function_lista_pedidos_por_prod(pedidos, nutr):

    pedidos_total = pd.DataFrame()

    # here we will append one table into another, one for each ingredient
    for value in nutr:
        # rename the columns
        pedidos_nutr = pedidos.rename(
            columns={value: 'produto', str(value+' Gr'): 'g'}
        )
        # Convert 'g' string to 0
        pedidos_nutr = pedidos_nutr[pd.to_numeric(
            pedidos_nutr['g'],
            errors='coerce'
            ).notnull()
        ]
        # convert 'g' to float
        pedidos_nutr['g'] = pedidos_nutr['g'].astype(float)
        # append all into a new df
        pedidos_total = pedidos_total.append(pedidos_nutr)

    # set index to 'produto'
    pedidos_total.set_index("produto")

    # the final table with all 'pedidos' and
    # a collumn 'produto' with the id for each one
    return pedidos_total


def function_produtos_por_nome(produtos):
    # rename 'unique id' to 'produtos'
    produtos = produtos.rename(columns={'_id': 'produto'})
    # ajustar o índice para 'produtos''
    produtos.set_index("produto")
    return produtos


# Function to sum all 'g' of each 'produto' and return the ordered total
def function_soma_pedidos_por_produto(df_pedidos, df_produtos, nutrientes):

    produtos = function_produtos_por_nome(df_produtos)

    # sum 'g' of 'produtos'
    pedidos_sum = df_pedidos.groupby(["produto"])["g"].sum()

    # merge produtos & pedidos by 'produto'
    result = pd.merge(pedidos_sum, produtos, on="produto")

    # print result
    return result


def function_mongo_filtered(coll, start=False, end=False):
    # test if daterange was insert, pipe mongo coll by date if true
    if not start and not end:
        start = datetime.now() - timedelta(7)
        start = start.isoformat()
        end = datetime.now().isoformat()

    pipe = [
        {
            "$match":
            {"Modified Date": {"$gte": start, "$lte": end}}
        },
        {
            "$sort":
            {"Modified Date": -1}
        }
    ]

    return coll.aggregate(pipe)


# Function to convert in 'produtos' the 'detalhes' from 'compra'

def produtos_from_detalhes(detalhes):
    # iterate over each 'pedidos' inside 'compras
    for pedidos_value in detalhes["pedidos"]:
        list
        # test if the is itens, do nothing if no
        try:
            pedidos_value["itens"]
        except Exception():
            pass
        else:
            # iterate over each 'item' inside a 'pedido'
            for items_value in pedidos_value["itens"]:
                list_produtos = []
                # convert the '_id' of each 'produto'
                # in the data from 'produto' collection
                for conteudo_value in items_value["conteúdo"]:
                    produto = client.grammo.produtos.find_one({
                        "_id": ObjectId(conteudo_value['produto'])
                        })
                    list_produtos.append(produto["dados"]["nome"])

    return detalhes


# FUNCTION TO RETURN A DICT WITH THE 'produtos'
# DETAILS BASED ON THE 'compra produto _id'

def produtos_from_compras(id_compra):
    pipe = [
        # match the 'compra' '_id'
        {
            "$match":
            {
                 "_id": ObjectId(id_compra)
            },
        },
        # unwind the doc till the 'conteúdo'
        {
            "$unwind": "$pedidos"
        },
        {
            "$unwind": "$pedidos.itens"
        },
        {
            "$unwind": "$pedidos.itens.conteúdo"
        },
        # Join the 'produtos' DB info by _id of each 'produto'
        {
            "$lookup":
            {
                "from": "produtos",
                "localField": "pedidos.itens.conteúdo.produto",
                "foreignField": "_id",
                "as": "produto_nome"
            },
        },
        # Join the 'categorias' DB info by _id of each 'produto'
        {
            "$lookup":
            {
                "from": "categorias",
                "localField": "produto_nome.dados.categoria",
                "foreignField": "_id",
                "as": "categoria"
            },
        },
        # Set the fields to show in the initial output
        {
            "$project":
            {
                "_id": 0,
                "num_pedido": "$pedidos.num",
                "num_item": "$pedidos.itens.num",
                "nome": "$produto_nome.dados.nome",
                "qtd": "$pedidos.itens.conteúdo.qtd",
                "ponto": "$pedidos.itens.conteúdo.ponto",
                "categoria": "$categoria.dados.nome",
                "entrega_hora": "$pedidos.entrega_hora",
            },
        },

        # convert the array in isolated values
        {
            "$unwind":
                {
                  "path": "$categoria",
                  "preserveNullAndEmptyArrays": True
                }
        },
        {
            "$unwind": "$nome"
        },
        # Regroup by 'pedido' and 'item'
        {
            "$group":
            {
                "_id": {
                    "pedido": "$num_pedido",
                    "item": "$num_item",
                },
                "conteúdo_item": {
                    "$push": '$$ROOT'
                },
                'item': {
                    "$addToSet": '$num_item',
                },
                'entrega_hora': {
                    "$addToSet": '$entrega_hora',
                }
            }
        },
        {
            "$unwind": "$item"
        },
        # Group by 'pedido'
        {
            "$group":
            {
                "_id": "$_id.pedido",
                "conteúdo_pedido": {
                    "$push": '$$ROOT'
                },
            }
        },
        # Sort by the 'entrega_hora'
        {
            "$sort":
                {"conteúdo_pedido.entrega_hora": 1}
        },


    ]

    compra_produtos = client.grammo.compras.aggregate(pipe)

    # organize a dict with 'pedidos' and 'itens' in nested format by number
    try:
        pedido_dict = {}
        for pedido in compra_produtos:
            item_dict = {}
            for conteúdo in pedido['conteúdo_pedido']:
                item_dict[conteúdo['_id']['item']] = conteúdo['conteúdo_item']
            pedido_dict[pedido['_id']] = item_dict
            pedido_dict[
                pedido['_id']
                ]['entrega_hora'] = conteúdo['entrega_hora']

        return pedido_dict
    except Exception():
        return False


def listen_pipes(start, end):
    listen_pipes = {}
    listen_pipes['compras'] = [
        {
            "$match":
            {
                 "bubble.created_date": {
                     "$gt": start,
                     '$lte': end,
                 }
            },
        },
        {
            "$lookup":
            {
                "from": "usr",
                "localField": "dados.usr",
                "foreignField": "_id",
                "as": "usuario"
            },
        },
        {
            "$lookup":
            {
                "from": "compra_num",
                "localField": "bubble._id",
                "foreignField": "_id",
                "as": "compra_num"
            },
        },
        {
            "$lookup":
            {
                "from": "nf",
                "localField": "compra_num.num",
                "foreignField": "compra_num",
                "as": "nf"
            },
        },
        {
            '$unwind':
                {
                    'path': '$nf',
                    'preserveNullAndEmptyArrays': True,
                }
        },
        {
            '$unwind': '$usuario'
        },
        {
            '$unwind': '$usuario.bio'
        },
        {
            "$project":
            {
                "_id": -1,
                "mongotime": {"$toDate": "$_id"},
                "número": "$compra_num.num",
                "bio": "$usuario.bio",
                "pgto_method": "$dados.pgto.method",
                "pgto_status": "$dados.pgto.status",
                "nf_num": "$nf._id",
                "bubble": 1,
            },
        }
    ]
    return listen_pipes
