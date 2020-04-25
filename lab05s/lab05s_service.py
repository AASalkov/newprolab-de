from flask import Flask, request
import requests
from urllib.parse import quote

app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello, world!'

@app.route('/get_gender_age', methods=['GET'])
def get_most_recent_stats_params():
    ga = request.args.get('ga')
    query = "SELECT uid from alexander_salkov_lab05s where gender_age == '{}'".format(ga.replace('"', ''))
    url = 'http://{}:{}/?query={}'.format("85.192.33.223", "8123", quote(query))
    response = requests.get(url)
    res = ""
    if response.status_code == 200:
        # parse output
        for rec in response.text.strip().split('\n'):
            res = res + '{"uid": "' + rec+ '"}' + "\n"
    print(res)
    return res

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')