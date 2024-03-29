import pathlib
import pymongo
from datetime import datetime
from kds.functions_mp import update_mp_status
from kds.functions_bubble import update_mongo, copy_bubble_all # noqa
from kds.config import Config

client = pymongo.MongoClient(Config.atlas_access)
db = client["bubble"]

bubble_things = [
    'agenda',
    'box',
    'categoria',
    'compra',
    'endereços',
    'pedido',
    'produto',
    'ponto',
    'pedidobox',
    'produto',
    'mov_moedas',
    'user'
]

# UPDATE ALL THE BUBBLE DB
for thing in bubble_things:
    update_mongo(db, thing)
copy_bubble_all()

# UPDATE THE MP STATUS BASED ON THE
# LAST UPDATE TIME SAVE IN THE RESPECTIVE FILE
cwd = pathlib.Path(__file__).parent.resolve()
with open(cwd / 'mp_lastupdate', 'r+') as f:
    lastupdate = datetime.fromisoformat(f.read())
    update_mp_status(lastupdate)
    f.seek(0)
    f.write(datetime.now().isoformat())
    f.close
