import sys
from flask import Flask

app = Flask(__name__)

import kds.views

if __name__ == "__main__":
    app.run()
