# import the main modules
import time
from datetime import datetime, timedelta

import pymongo
from bson.json_util import dumps
# import the modules libraries / functions / classes
from flask import (Response, flash, redirect, render_template, request,  # noqa
                   stream_with_context, url_for)
from flask_bootstrap import Bootstrap
from flask_datepicker import datepicker
from flask_login import (LoginManager, current_user, login_required,  # noqa
                         login_user, logout_user)
from werkzeug.security import check_password_hash
from werkzeug.urls import url_parse

import kds.functions_bubble
import kds.functions_external
import kds.functions_internal
import kds.functions_mp
import kds.functions_nf
import kds.functions_view
import kds.heatmap
# import the grammo apps / libraries / functions / classes
from kds import app
from kds.api.api import api_bp
from kds.config import Config
from kds.forms import LoginForm

# configure the app com resources
app.config.from_object(Config)
app.register_blueprint(api_bp, url_prefix='/api')

Bootstrap(app)
datepicker(app)

# set the database
client = pymongo.MongoClient(Config.atlas_access)
# db = client["bubble"]

# Call the login manager
login_manager = LoginManager(app)
login_manager.login_view = 'login'

tmp = list(client.grammo.nf.find())

# convert in Pandas DF the Mongo Collections
nutrientes = ['Carbo', 'Grão', 'Legumes', 'Proteina']

# set range date (default 1 week before -- today)
start_date = datetime.now() - timedelta(7)
last24h = datetime.now() - timedelta(hours=24)
end_date = datetime.now()
tomorrow24h = datetime.now() + timedelta(hours=24)
range_date = {
    #    'start': start_date.strftime("%d/%m/%Y"),
    #    'end': end_date.strftime("%d/%m/%Y"),
    #    'last24h': last24h.strftime("%d/%m/%Y"),
    #    'now': datetime.now().strftime("%d/%m/%Y"),
    #    'tomorrow24h': tomorrow24h.strftime("%d/%m/%Y"),
    'now': datetime.now().isoformat(),
    'tomorrow24h': tomorrow24h.isoformat(),
    'last24h': last24h.isoformat(),
    'start': start_date.isoformat(),
    'end': end_date.isoformat(),
    }

##############################


@app.route("/")
@login_required
def index():
    return render_template("home.html", range_date=range_date)

##############################


@app.route("/dev")
@login_required
def dev():
    out = []
    for doc in client.grammo.nutr_TACO.find():
        out.append(doc['nome_tec'])
    lista = list(out)
    return render_template("dev.html", lista=lista)

##############################


@app.route("/heatmap_grammo")
@login_required
def heatmap_grammo():
    data = client.bubble.endereços.find()
    map = kds.heatmap.heatmap(data)
    return map

##############################


@app.route("/lista_clientes")
@login_required
def lista_clientes():
    mongo_filtered = kds.functions_view.function_mongo_filtered(
        client.bubble.user,
        request.args.get('start'),
        request.args.get('end')
        )
    return render_template('lista_clientes.html', clientes=mongo_filtered)

##############################


@app.route("/compra_detalhe", methods=['POST', 'GET'])
@login_required
def compra_detalhe():

    compra_num = int(request.args.get('compra_num'))
    # get the compra_num from Bubble if is under 10000
    bubble_id = client.grammo.compra_num.find_one({'num': compra_num})["_id"]

    # grab 'detalhes' to format the compra info
    pedidos = kds.functions_internal.compra_lookup(bubble_id)
    pedidos[0]['compra_num'] = compra_num

    return render_template('compra_detalhe.html', pedidos=pedidos)

##############################


class User:
    def __init__(self, username):
        self.username = username

    @staticmethod
    def is_authenticated():
        return True

    @staticmethod
    def is_active():
        return True

    @staticmethod
    def is_anonymous():
        return False

    def get_id(self):
        return self.username

    @staticmethod
    def check_password(password_hash, password):
        return check_password_hash(password_hash, password)

    @login_manager.user_loader
    def load_user(username):
        u = client.grammo.users.find_one({"username": username})
        if not u:
            return None
        return User(username=u['username'])

    @app.route('/login', methods=['GET', 'POST'])
    def login():
        if current_user.is_authenticated:
            return redirect(url_for('index'))
        form = LoginForm()
        if form.validate_on_submit():
            user = client.grammo.users.find_one(
                {"username": form.username.data}
                )
            if user and User.check_password(
                    user["password"], form.password.data
                    ):
                user_obj = User(username=user["username"])
                login_user(user_obj)
                flash("Login bem sucedido!")
                next_page = request.args.get('next')
                if not next_page or url_parse(next_page).netloc != '':
                    next_page = url_for('index')
                return redirect(next_page)
            else:
                flash("Usuário ou senha inválidos")
        return render_template('login.html', title='Sign In', form=form)

    @app.route('/logout')
    def logout():
        logout_user()
        return redirect(url_for('index'))

##############################


@app.route("/realtime_compras", methods=['POST', 'GET'])
@login_required
def realtime_compras():

    # If POST for "compras", save the inputs
    if request.method == "POST" and request.form.get('dest') == 'compras':

        # Get the args from the POST
        compra_num = request.form.get('num')
        compra_class = request.form.get('class')
        compra_value = request.form.get('value')
        bubble_id = client.grammo.compra_num.find_one(
            {'num': int(compra_num)}
            )['_id']

        # Update the MongoDB
        client.grammo.compras.update_one(
            {'bubble._id': bubble_id},
            {
                "$set":
                {
                    'dados.pgto.'+compra_class: compra_value
                }
            },
            upsert=False)

        return Response(status=200)

    # If POST for "NF", save the inputs
    if (request.method == "POST" and request.form.get('dest') == 'NF'):
        try:
            nf_num = kds.functions_nf.NF_send(
                int(request.form.get('compra_num'))
                )
        except Exception as e:
            print(f"[!] Exception caught: {e}")

        return Response(str(nf_num), status=200)

    # else show the realtime page
    else:
        return render_template("realtime_compras.html")

##############################


@app.route("/listen", methods=['GET'])
@login_required
def listen():
    def respond_to_client():

        coll = request.args.get('coll')
        start = request.args.get('start')
        end = request.args.get('end')

        realtime_stamp = ''

        while True:
            last_mongo_date_stamp = kds.functions_internal.mongo_updated_time(coll) # noqa

            if (last_mongo_date_stamp != realtime_stamp):

                pipe = kds.functions_view.listen_pipes(start, end)[coll]
                out = list(client.grammo[coll].aggregate(pipe))
                data = dumps(out)
                yield f"data: {data}\nevent: online\n\n"

            realtime_stamp = last_mongo_date_stamp
            time.sleep(15)

    return app.response_class(
        stream_with_context(respond_to_client()),
        mimetype='text/event-stream'
        )


##############################


@app.route("/insta_er")
@login_required
def insta_er():
    target_profile = request.args.get('profile')
    er = kds.functions_internal.insta_engagement_rate(target_profile)
    return er
