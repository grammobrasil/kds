import pymongo
from datetime import datetime
import requests

from kds.config import Config
client = pymongo.MongoClient(Config.atlas_access)


def read_mp(start):

    end = datetime.now().astimezone().isoformat("T", "milliseconds")
    url = "https://api.mercadopago.com/v1/payments/search"

    # Query initial parameters.
    # We do not send a limit parameter (default is 100)
    output = []

    offset = 0
    remaining = 30

    while remaining > 0:

        payload = {
            'sort': 'date_created',
            'criteria': 'desc',
            'range': 'date_created',
            'begin_date': start,
            'end_date': end,
            'offset': offset,
        }

        headers = {
            'Authorization': Config.MP_API_KEY
        }

        response = requests.request(
            "GET",
            url,
            headers=headers,
            params=payload
            )
        result = response.json()['results']
        total = response.json()['paging']['total']
        remaining = total - offset

        offset += 30
        output += result

    return output


def update_mongo_mp(mongo_col):

    # get the last date from mongo collection
    pipe = [
        {
            "$sort":
                {"date_created": -1}
        },
        {
            "$project":
                {
                    "date_created": 1
                },
        }
    ]

    # chech last date updated in the mongo collection
    out = mongo_col.aggregate(pipe)
    out_list = list(out)
    updated_date = out_list[0]['date_created']

    # read bubble bi API call
    mp_data = read_mp(updated_date)

    # if result, insert into mongo
    if mp_data:
        for doc in mp_data:
            mongo_col.replace_one({"id": doc["id"]}, doc, upsert=True)
        return "updated!"
    else:
        return "no outdated data"


def read_mp_compra(data):

    url = "https://api.mercadopago.com/v1/payments/search"

    # Query initial parameters.
    # We do not send a limit parameter (default is 100)

    payload = {
        'external_reference': data
    }

    headers = {
        'Authorization': Config.MP_API_KEY
    }

    response = requests.request("GET", url, headers=headers, params=payload)
    return response.json()['results']


def update_mp_status(start):

    pipe = [
        {
            '$addFields': {
                "mongotime": {
                    "$toDate": "$_id",
                }
            }
        },
        {
            "$match": {
                "mongotime": {"$gte": start}
            }
        }
    ]

    for doc in client.grammo.compras.aggregate(pipe):
        bubble_id = doc['bubble']['_id']
        statusmp = read_mp_compra(bubble_id)
        if statusmp:
            dict_pgto = {}
            dict_pgto.update({'method': 'mp'})
            dict_pgto.update({'status': statusmp[0]["status"]})
            dict_pgto.update(
                {'value_total': statusmp[0]
                    ["transaction_details"]["total_paid_amount"]}
                )
            dict_pgto.update(
                {'value_net': statusmp[0]
                    ["transaction_details"]["net_received_amount"]}
                )

            try:
                client.grammo.compras.update_one(
                    {
                        'bubble._id': bubble_id},
                    {
                        '$set': {'dados.pgto': dict_pgto}},
                    upsert=True)
            except Exception():
                return 'Erro salvando dados'
        else:
            None

    return 'MP status updated!'
