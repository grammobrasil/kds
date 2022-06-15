import pymongo
from datetime import datetime, timedelta
import requests
import json
from bson.json_util import dumps
from bson.objectid import ObjectId
import urllib

from config import Config
client = pymongo.MongoClient(Config.atlas_access)

# function to extract API from bubble
def read_bubble(thing, constraints):

    session = requests.Session()

    #API KEY as defined in the settings tab, so that the script has full access to the data
    API_KEY = Config.bubble_API_KEY
    base_url = 'https://grammo.app/api/1.1/obj/' +  thing

    # Query initial parameters. We do not send a limit parameter (default is 100)
    dict_pedidoboxput=[]
    cursor = 0
    remaining = 100

    while remaining > 0:
        #data we send with the search. Search constraints would be here
        params = {
            'constraints': json.dumps(constraints),
            'sort_field': 'Modified Date',
            'descending': 'true',
            'cursor': cursor,
            'api_token': API_KEY
        }

        url = base_url + '?' + urllib.parse.urlencode(params)
        response = session.get(url)

        if response.status_code != 200:
            print('Error with status code {}'.format(response.status_code))
            exit()

        chunk = response.json()['response']
        remaining = chunk['remaining']
        count = chunk['count']
        results = chunk['results']
        dict_pedidoboxput += results
        cursor += count

    return dict_pedidoboxput

def read_bubble_id(thing, _id):

    session = requests.Session()

    #API KEY as defined in the settings tab, so that the script has full access to the data
    API_KEY = Config.bubble_API_KEY
    base_url = 'https://grammo.app/api/1.1/obj/' +  thing

    params = {
        'api_token': API_KEY
    }

    url = base_url + '/' + _id + '?' + urllib.parse.urlencode(params)
    response = session.get(url)

    if response.status_code != 200:
        print(_id)
        print('Error with status code {}'.format(response.status_code))        
        exit()

    return response.json()['response']

# function to update mongo from bubble
def update_mongo(db, thing):
    mongo_col = db[thing]
    
    # get the last date from mongo collection
    updated_date = get_db_lastdate(mongo_col)
    
    # limit the bubble call by updated date
    constraints = [{"key": "Modified Date", "constraint_type": "greater than","value": updated_date}]

    # read bubble by API call
    bubble_col = read_bubble(thing, constraints)    
    
    # if result, insert into mongo
    if bubble_col:
        for doc in bubble_col:
            mongo_col.replace_one({"_id": doc["_id"]}, doc, upsert=True)


    
# function to get mongo collection last date updated
def get_db_lastdate(col):

    # fields to pipe the aggragate
    pipe = [
        {
            "$addFields": 
                {"convertedDate": { "$toDate": '$Modified Date'}} 
        },
        {
            "$sort":
                {"convertedDate": -1}
        },
        {
            "$project":
                {"_id": 0,
                 "nome" : 1,
                "Creation Date" : 1,
                "Modified Date" : 1,
                "convertedDate" : 1
                },
        }
    ]

    # try to see if theres is a date
    try:
        dict_pedidobox = col.aggregate(pipe)
        dict_pedidobox_list = list(dict_pedidobox)
        return dict_pedidobox_list[0]['Modified Date']

    # return 01/01/2020 if not (to get all data from Grammo begining in Bubble
    except:
        return datetime(2020, 1, 1).strftime("%c")


def set_compra_numero():
    
    # get the last 'compra_num' from the DB
    last_compra = list(client.grammo.compra_num.aggregate([{"$sort":{"created_date": -1}}]))[0]
    num = last_compra['num']

    pipe = [
        {"$match":
                {"Created Date": {"$gt": last_compra["created_date"]}}
        },
        {"$sort":
                {"Created Date": 1}
        }
        ]

    # insert into 'compra_num' a new doc with the new 'compra'
    for doc in client.bubble.compra.aggregate(pipe):
        num += 1
        client.grammo.compra_num.insert_one({
                "_id": doc["_id"],
                "created_date": doc['Created Date'],
                "num": num
            })

def get_date_grammo(col):
    # pipe the grammo col to get last modified date
    pipe_grammo = [{"$sort": {"bubble.modified_date": -1}}]
    date_grammo = list(client.grammo[col].aggregate(pipe_grammo))[0]["bubble"]["modified_date"]
    return date_grammo

def copy_bubble_endereços(bubble_id):
    # search the bubble col by de 'id' provided
    array_end = []
    for doc in client.bubble.endereços.find({"Created By": bubble_id}):
        end = {
            '_id': ObjectId(),
            'bubble':   {
                '_id': doc['_id'],
                'created_date': doc['Created Date'],
                'modified_date': doc['Modified Date'],
                },
            'nome': doc['Nome'],
            'geo': doc['Geo']['address'],
            'complemento': doc['Complemento'],
            'lat': doc['Geo']['lat'],
            'lng': doc['Geo']['lng'],

        }
        array_end.append(end)

    client.grammo.usr.update_one(
        {'bubble._id': bubble_id},
        {'$set': {'endereços': array_end}}
    , upsert=True)

def copy_bubble_usr(last_date):

    # pipe bubble with last results
    pipe_bubble = [{"$match": {"Created Date": {"$gt":last_date}}}]

    for doc in client.bubble.user.aggregate(pipe_bubble):
        
        # check if fields exists, set empty if not
        try: DDD = doc['DDD']
        except: DDD = ''
        
        try: phone = doc['FONE']
        except: phone = ''
        
        try: CPF = doc['CPF']
        except: CPF = ''

        try: DN = doc['DATA DE NASCIMENTO']
        except: DN = ''
        
        # update 'bio', 'bubble' and 'contato'
        client.grammo.usr.update_one(
            {'bubble._id': doc["_id"]},
            {"$set": 
                 {
                    'bubble':
                        {
                            '_id': doc['_id'],
                            'created_date': doc['Created Date'],
                            'modified_date': doc['Modified Date'],
                        },
                    'bio': 
                        {
                            'nome': doc['nome'],
                            'apelido': doc['apelido'],
                            'DN': DN,
                            'CPF': CPF,
                        },
                    'contato':
                        {
                            'DDD': DDD,
                            'fone': phone,
                            'email': doc['authentication']['email']['email'],
                        },
                }
            }
            , upsert=True)
        
        # uddate 'usr' - 'endereços'
        copy_bubble_endereços(doc["_id"])

    return "'Usr' successfully updated!"

def copy_bubble_categorias(last_date):
    # pipe bubble with result newer than the provided date
    pipe_bubble = [{"$match": {"Created Date": {"$gt":last_date}}}]

        
    # insert into the col
    for doc in client.bubble.categoria.aggregate(pipe_bubble):
        client.grammo.categorias.update_one(
            {'bubble._id': doc["_id"]},
            {"$set": 
                 {
                    'bubble':
                        {
                            '_id': doc['_id'],
                            'created_date': doc['Created Date'],
                            'modified_date': doc['Modified Date'],
                        },
                    'dados': 
                        {
                            'nome': doc['nome'],
                        },

                }
            }
            , upsert=True)
    return "'Categorias' successfully updated!"

def copy_bubble_produtos(last_date):
    # pipe bubble with result newer than the provided date
    pipe_bubble = [{"$match": {"Created Date": {"$gt":last_date}}}]

    # insert into the col
    for doc in client.bubble.produto.aggregate(pipe_bubble):
        # set the id of the 'categoria' from grammo DB
        try: categ = client.grammo.categorias.find_one({'bubble._id':doc['categoria']})["_id"]
        except: categ = ''

        try: nome = doc['Nome']
        except: compra_num = ''
        
        client.grammo.produtos.update_one(
            {'bubble._id': doc["_id"]},
            {"$set": 
                 {
                    'bubble':
                        {
                            '_id': doc['_id'],
                            'created_date': doc['Created Date'],
                            'modified_date': doc['Modified Date'],
                        },
                    'dados': 
                        {
                            'nome': nome,
                            'categoria': categ
                        },

                }
            }
            , upsert=True)
    return "'Produtos' successfully updated!"

def set_compra_numero_all():
    
    # get the last 'compra_num' from the DB
    last_compra = list(client.grammo.compra_num.aggregate([{"$sort":{"created_date": -1}}]))[0]
    num = last_compra['num']

    # pipe the 'compras' col for docs withdict_pedidobox 'compra_num'
    pipe = [
        {"$match":
                {"Created Date": {"$gt": last_compra["created_date"]}}
        },
        {"$sort":
                {"Created Date": 1}
        }
        ]

    for doc in client.bubble.compra.aggregate(pipe):
        # set the nummber to the actual number of docs in 'compras' col + 1 and save it
        num += 1
        client.grammo.compra_num.insert_one({
                "_id": doc["_id"],
                "created_date": doc['Created Date'],
                "num": num
            })

def copy_bubble_compras(last_date):
    # pipe bubble with last results
    pipe_bubble = [{"$match": {"Modified Date": {"$gt":last_date}}}]

    for compra in client.bubble.compra.aggregate(pipe_bubble):
        # set the id of the usr from grammo DB
        try: usr = client.grammo.usr.find_one({'bubble._id':compra['comprador']})["_id"]
        except: usr = ''

        dict_compra_bubble = {}
        
        dict_compra_bubble.update({'_id': compra['_id']})
        dict_compra_bubble.update({'created_date': compra['Created Date']})
        dict_compra_bubble.update({'modified_date': compra['Modified Date']})

        # check if multiple fields exists, set empty if not
        bubble_fields = [
            'pedidos',
            'compra num',
            'comprador',
            'valor total',
            'cobrar',
            'forma',
            'cupom',
            'cancelado',
            'liberado',
            'pago',
            'status',
            'validado',
        ]

        for field in bubble_fields:
            try: compra[field]
            except: pass
            else: dict_compra_bubble.update({field : compra[field]}) if compra[field] != 0 else None


        try: compra["pedidos"]
        except: pass
        else:
            # create a list of 'pedidos'
            list_pedido = []
            
            for index, value in enumerate(sorted(compra["pedidos"])):
                # set the 'pedido' number
                pedido_numero = index+1

                # search in bubble for the specific 'pedido'
                pedido_body = client.bubble.pedido.find_one({"_id": value})
                if pedido_body: pass
                else:
                # test if the 'pedido' exists in bubble, else exit loop
                    try: pedido_body = read_bubble_id('pedido', value)
                    except: break
                
                # create a dict of the 'pedidos' with bubble content
                dict_pedido_bubble = {}

                dict_pedido_bubble.update({'_id': pedido_body['_id']})
                dict_pedido_bubble.update({'created_date': pedido_body['Created Date']})
                dict_pedido_bubble.update({'modified_date': pedido_body['Modified Date']})
                
                try: pedido_body['pedidos box']
                except: pass
                else: dict_pedido_bubble.update({'pedidos_box' : pedido_body['pedidos box']}) if pedido_body['pedidos box'] != 0 else None

                try: pedido_body['km']
                except: pass
                else: dict_pedido_bubble.update({'km' : pedido_body['km']}) if pedido_body['km'] != 0 else None

                try: pedido_body['valor entrega']
                except: pass
                else: dict_pedido_bubble.update({'valor_entrega' : pedido_body['valor entrega']}) if pedido_body['valor entrega'] != 0 else None

                # create a dict for the 'pedido' with the details from it
                dict_pedido = {}
                
                # Insert into pedidos the bubble dict
                dict_pedido.update({'bubble': dict_pedido_bubble})

                # set the number for the 'pedido'
                dict_pedido.update({'num': pedido_numero})

                # get the full datetime for schedulled 'pedido'
                dict_pedido.update({'entrega_hora': get_datetime_from_pedido(pedido_body).isoformat()}) if get_datetime_from_pedido(pedido_body) != 'PE' else None
                
                try: pedido_address = get_usr_address_from_bubble_geo(usr, pedido_body['endereço']['address'])
                except: pass
                else: dict_pedido.update({'endereço' : pedido_address}) if pedido_body['endereço'] != 0 else None
                
                # update itens from 'pedidosbox'
                try: pedidoxbox = client.bubble.pedido.find_one({"_id":value})['pedidos box']
                except: pass
                else:
                    list_itens = []
                    for boxindex, boxvalue in enumerate(sorted(pedidoxbox)):
                        # set the 'pedido' number
                        pedidobox_numero = boxindex+1

                        # search in bubble for the specific 'pedidobox'
                        pedidobox_body = client.bubble.pedidobox.find_one({"_id": boxvalue})
                        if pedidobox_body: pass
                        else:
                        # test if the 'pedido' exists in bubble, else exit loop
                            try: pedidobox_body = read_bubble_id('pedidobox', boxvalue)
                            except: break

                        # create a dict of the 'pedidos' with bubble content
                        dict_pedidobox_bubble = {}

                        dict_pedidobox_bubble.update({'_id': pedido_body['_id']})
                        dict_pedidobox_bubble.update({'created_date': pedido_body['Created Date']})
                        dict_pedidobox_bubble.update({'modified_date': pedido_body['Modified Date']})

                        try: pedidobox_body['valor box']
                        except: pass
                        else: dict_pedidobox_bubble.update({'valor_box': pedidobox_body['valor box']})
                        
                        # create a dict of the 'item' conteúdo
                        dict_pedidobox = {}
                        
                        # Insert into pedidosbpx the bubble dict
                        dict_pedidobox.update({'bubble': dict_pedidobox_bubble})
                
                        dict_pedidobox.update({'num': pedidobox_numero})
                        
                        # Create a list for the 'conteúdo'
                        list_conteudo = []
                        
                        # insert into the list if present for each
                        try: pedidobox_body['Proteina']
                        except: pass
                        else: 
                            try: pedidobox_body['ponto 1']
                            except: pedidobox_body_pt = ''
                            else: pedidobox_body_pt = pedidobox_body['ponto 1']

                            proteina = client.grammo.produtos.find_one({"bubble._id": pedidobox_body['Proteina']})
                            list_conteudo.append({'produto': proteina["_id"], 
                                "qtd": pedidobox_body['Proteina Gr'],
                                "ponto": pedidobox_body_pt
                                }) if pedidobox_body['Proteina Gr'] != 0 else None

                        try: pedidobox_body['Carbo']
                        except: pass
                        else:
                            carbo = client.grammo.produtos.find_one({"bubble._id": pedidobox_body['Carbo']})
                            list_conteudo.append({'produto': carbo["_id"],  "qtd": pedidobox_body['Carbo Gr'] }) if pedidobox_body['Carbo Gr'] != 0 else None

                        try: pedidobox_body['Grão']
                        except: pass
                        else:
                            grao = client.grammo.produtos.find_one({"bubble._id": pedidobox_body['Grão']})
                            list_conteudo.append({'produto': grao["_id"],  "qtd": pedidobox_body['Grão Gr'] }) if pedidobox_body['Grão Gr'] != 0 else None

                        try: pedidobox_body['Legumes']
                        except: pass
                        else:
                            legumes = client.grammo.produtos.find_one({"bubble._id": pedidobox_body['Legumes']})
                            list_conteudo.append({'produto': legumes["_id"],  "qtd": pedidobox_body['Legumes Gr'] }) if pedidobox_body['Legumes Gr'] != 0 else None

                        try: pedidobox_body['salada']
                        except: pass
                        else:
                            salada = client.grammo.produtos.find_one({"bubble._id": pedidobox_body['salada']})
                            try: pedidobox_body['gr salada']
                            except: salada_gr = 0
                            else: salada_gr = pedidobox_body['gr salada']
                            list_conteudo.append({'produto': salada["_id"],  "qtd": salada_gr })

                        try: pedidobox_body['bebidas']
                        except: pass
                        else:
                            bebida = client.grammo.produtos.find_one({"bubble._id": pedidobox_body['bebidas']})
                            list_conteudo.append({'produto': bebida["_id"],  "qtd": 1})
                            
                        try: pedidobox_body['Azeite']
                        except: pass
                        else: list_conteudo.append({'produto': "62047b931835f23c7dbf211d",  "qtd": 1 })
        
                        try: pedidobox_body['Vinagre']
                        except: pass
                        else: list_conteudo.append({'produto': "62047bfd1835f23c7dbf2124",  "qtd": 1 })

                        try: pedidobox_body['talheres']
                        except: talheres_value = ''
                        else: list_conteudo.append({'produto': "62047c231835f23c7dbf2126",  "qtd": 1 })
                        
                        # insert the contet of each 'conteúdo' inside a list from the 'pedidobox' dict
                        dict_pedidobox['conteúdo'] = list_conteudo
                        
                        # append to a list of itens
                        list_itens.append(dict_pedidobox)
                        
                    # inser the list of 'itens' of the 'pedido' to the dict 'itens' value
                    dict_pedido['itens'] = list_itens

                # append the 'pedido' dict to a list of 'pedidos'
                list_pedido.append(dict_pedido)
        
            # update mongo
            client.grammo.compras.update_one(
                    {'bubble._id': compra["_id"]},
                    {"$set": 
                        {
                        'bubble': dict_compra_bubble,
                        'dados': {'usr': usr},
                        'pedidos': list_pedido,
                        }
                    }
                    , upsert=True)

    return "'Compras' successfully updated!"

## FUNCTION TO GET THE GEO ADDRESS FROM A BUBBLE VALUE TO THE 'usr' Grammo DB

def get_usr_address_from_bubble_geo(usr_obj_id, bubble_geo_add):
    # Pipe to match the user ObjectId AND the respective geo address
    pipe = [
        {
            "$match": {
                "_id": usr_obj_id
            }
        },
        {
            "$unwind": '$endereços'
        },
        {
            "$match": {
                "endereços.geo": bubble_geo_add
            }
        },

    ]

    # return the first ocurrence ([0]) in the given list, assuming all the values will be the same
    end_out = {}
    end_out['_id'] = list(client.grammo.usr.aggregate(pipe))[0]['endereços']['_id']
    end_out['bubble'] = list(client.grammo.usr.aggregate(pipe))[0]['endereços']['bubble']
    
    return end_out

    
### FUNCTION TO THE THE FULL DATETIME FROM ANY SCHEDULLED PEDIDO IN Bubble 'pedidos' DATA, RETURNS 'PE' ('pronta entrega') IF NOT

def get_datetime_from_pedido(pedido):
    # test if 'entrega hora' exists, meaning that is a scheduled delivery
    try: 
        pedido['entrega hora']
    # if doesnt exist, it PE ('pronta entrega')
    except: 
        entrega_hora = 'PE'
    else:
        # test if 'entrega hora' its in date format, meaning it's '50 minutes' from the created date, in other words an PE
        try: 
            datetime.strptime(pedido['entrega hora'], '%b %d, %Y %H:%M %p')
        except: 
            # it 'entrega hora' contains 'Produzir agora' it's also PE 
            if (pedido['entrega hora'] == 'Produzir agora.'):
                entrega_hora = 'PE'
            else:
            # if not, 'entrega hora' is a string with time values of the delivery schedule
                entrega_hora = pedido['entrega hora']
        # here the result from the test from date format ('PE' if passed)
        else:
            entrega_hora = 'PE'

    # now get the date and hour from scheduled 'pedido' if its not PE
    if entrega_hora != 'PE':
        # get the full date from 'dia entrega', leave empty if fail
        try: entregaD = datetime.strptime(pedido['dia entrega'], '%Y-%m-%dT%H:%M:%S.%f%z')
        except: pass
        # if not, get hour and minute from the 'entrega hora' string
        else:
            entregaH = pedido['entrega hora'][0:2]
            entregaM = pedido['entrega hora'][3:5]
            # set the 'entrega hora' as a full time with the ajusted values
            entrega_hora = entregaD.replace(hour=int(entregaH), minute=int(entregaM), second=0, microsecond=0)

    # Finally, returns the full time if its schedulled or 'PE' if not
    return entrega_hora

# FUNCTION TO UPDATE ALL THE DB
def copy_bubble_all():    
    copy_bubble_usr(get_date_grammo('usr'))
    copy_bubble_categorias(get_date_grammo('categorias'))
    copy_bubble_produtos(get_date_grammo('produtos'))
    copy_bubble_compras(get_date_grammo('compras'))
    set_compra_numero_all()

# FUNCTION TO GET A SPECIFIC 'compra' by its number
def get_compra_by_num(num):
    compra_num = client.grammo.compra_num.find_one({"num": num})
    compra = client.grammo.compras.find_one({"bubble._id": compra_num["_id"]})
    return compra

