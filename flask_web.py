from flask import Flask, jsonify
import psycopg2 as pg2

app = Flask(__name__)


@app.route('/', methods=['GET'])
def hello_world():
    return jsonify({
        "name": "sakda",
        "address": "plamplace"
    })


# @app.route('/api/getdata', methods=['GET'])
# def

if __name__ == '__main__':
    app.run(host="127.0.0.1", port=4000)
