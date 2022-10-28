from flask import Flask

app = Flask(__name__)

import kds.views # noqa

if __name__ == "__main__":
    app.run()
