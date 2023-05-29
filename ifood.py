from requests import request
from pprint import pprint  # noqa
import json  # noqa
import time

from kds.config import Config

ids = {
    'merchant_id': 'a826ba41-67e5-4f5d-87bb-f682749803f2',
    'clientId': 'acd5908a-a113-4a0c-a6fc-d3902cfde3e4',
    'clientSecret': 'lnimef2pacpwlm6t2ej6xdwy5gy77gtwcykkm'
    + 'xghbbjwbromk7cc07ct1ec91z9yq9mduwm3h4zit9hes0ijgu3g0x2iuppd1pa',
}

url = {
    'base': 'https://merchant-api.ifood.com.br/',
    'server': {
        'auth': 'authentication/v1.0/',
        'merchant': 'merchant/v1.0/',
        'order': 'order/v1.0/',
        'shipping': 'shipping/v1.0/',
    }
}

url.update({
    'token': url['base'] + url['server']['auth'] + 'oauth/token/',
    'merchant_status': url['base'] + url['server']['merchant']
    + 'merchants/{merchantId}/status'.format(
      merchantId=Config.ifood['merchant_id']),
    'event_polling': url['base'] + url['server']['order'] + 'events:polling',
    'order_id': url['base'] + url['server']['order'] + 'orders/',
    'ack': url['base'] + url['server']['order'] + 'events/acknowledgment',
    'shipping': url['base'] + url['server']['shipping']
    + 'merchants/{merchantId}/'.format(merchantId=Config.ifood['merchant_id']),
})


def get_token():
  payload_get_token = 'grantType=client_credentials&' \
        + 'authorizationCode=&authorizationCodeVerifier=&refreshToken'\
        + '&clientId=' + Config.ifood['clientId'] \
        + '&clientSecret=' + Config.ifood['clientSecret']

  headers_get_token = {
      'accept': 'application/json',
      'Content-Type': 'application/x-www-form-urlencoded'
  }

  response_token = request(
      'POST', url['token'],
      headers=headers_get_token,
      data=payload_get_token)
  
  return response_token

token = get_token()
print(token.json()['expiresIn'])

expires_in = token.json()['expiresIn']
expiry_time = time.time() + expires_in
dt = datetime.fromtimestamp(expiry_time)
iso_string = dt.isoformat()

print(iso_string)


while True:
    # check if the token has expired
    if time.time() >= expiry_time:
        # renew token
        response = requests.post(token_url, data=token_params)
        token = response.json()['access_token']
        expires_in = response.json()['expires_in']
        expiry_time = time.time() + expires_in

    # use the token for API requests
    response = requests.get(api_url, headers={'Authorization': f'Bearer {token}'})
    # process response as needed

    # wait for some time before making the next API request
    time.sleep(1)

# make a function to update the token  if it's expired
# if token['expires_in'] < 0:
#   token = get_token()

def update_token():
  global token
  token = get_token()['accessToken']



pprint(response_token.json())
token = response_token.json()['accessToken']
print(response_token.json())

payload = {}
headers = {
    'accept': 'application/json',
    'Authorization': 'Bearer ' + token,
    'Content-Type': 'application/json',
}

merchant_status = request(
    'GET', url['merchant_status'],
    headers=headers,
    data=payload)
pprint(merchant_status.json())

polling = request('GET', url['event_polling'], headers=headers, data=payload)
print(polling.json())


def order_go(order_id, set):
    return request(
        set['method'],
        url['order_id'] + order_id + set['action'],
        headers=headers,
        data=None)


order_set = {
    0: {
        'action': '/',
        'method': 'GET',
        },
    1: {
        'action': '/confirm',
        'method': 'POST',
        },
    2: {
        'action': '/readyToPickup',
        'method': 'POST',
        },
    3: {
        'action': '/requestDriver',
        'method': 'POST',
        },
    4: {
        'action': '/dispatch',
        'method': 'POST',
        },
    5: {
        'action': '/tracking',
        'method': 'GET',
        },
}


cliente_teste = {
    'nome': 'João da Silva',
    'end': 'R. Expedito Pereira de Souza',
    'num': '100',
    'cidade': 'Bujari',
    'UF': 'AC',
    'CEP': '69923-000',
    'Latitude': -9.8234667,
    'Longitude': -67.9530532,
}

ship_options = request(
    'GET', url['shipping']
    + '/deliveryAvailabilities'
    + '?Latitude={Latitude}&Longitude={Longitude}'.format(**cliente_teste),
    headers=headers)
print(ship_options.json())

order_test = {
  'customer': {
    'name': cliente_teste['nome'],
    'phone': {
      'countryCode': '55',
      'areaCode': '51',
      'number': '997998263'
    }
  },
  'delivery': {
    'merchantFee': 0,
    'deliveryAddress': {
      'postalCode': cliente_teste['CEP'],
      'streetNumber': cliente_teste['num'],
      'streetName': cliente_teste['end'],
      'complement': '-',
      'neighborhood': '-',
      'city': cliente_teste['cidade'],
      'state': cliente_teste['UF'],
      'country': 'BR',
      'reference': '-',
      'coordinates': {
        'latitude': cliente_teste['Latitude'],
        'longitude': cliente_teste['Longitude']
      }
    }
  },
  'items': [
    {
      'id': '3fa85f64-5717-4562-b3fc-2c963f66afa6',
      'name': 'Box de Salmão',
      'externalCode': '123',
      'quantity': 1,
      'unitPrice': 30,
      'price': 30,
      'optionsPrice': 0,
      'totalPrice': 30,
      'options': []
    }
  ],
  'payments': {
    'methods': [
      {
        'method': 'CREDIT',
        'type': 'OFFLINE',
        'value': 30,
        'card': {
          'brand': 'VISA'
        }
      }
    ]
  },
  'metadata': {
    'additionalProp1': '123',
    'additionalProp2': 'Testando',
    'additionalProp3': 'Grammo'
  }
}

ship_post = request(
    'POST',
    url['shipping'] + 'orders',
    headers=headers,
    data=json.dumps(order_test)
    )

print(ship_post.json())

pedido_id = ship_post.json()['id']
go = order_go(pedido_id, order_set[4])
pprint(go.json())
pprint(order_test)
print(token)
