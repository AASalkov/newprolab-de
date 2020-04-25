import json
from flask import Flask, request
from joblib import load
import pandas as pd
import re
from urllib.parse import urlparse
from urllib.request import urlretrieve, unquote

def url2domain(url):
    url = re.sub('(http(s)*://)+', 'http://', url)
    parsed_url = urlparse(unquote(url.strip()))
    if parsed_url.scheme not in ['http','https']: return None
    netloc = re.search("(?:www\.)?(.*)", parsed_url.netloc).group(1)
    #if netloc is not None: return str(netloc.encode('utf-8')).strip()
    if netloc is not None: return str(netloc).strip()
    return None

def list2domain(rec):
    result = []
    for x in rec:
       result.append(url2domain(x["url"]))
    return ', '.join(result)

print("Start")
model = load("lab05.joblib")
app = Flask(__name__)

@app.route('/')
def hello_world():
    return 'Hello, world!'

@app.route('/predict_gender_age', methods=['POST'])
def predict_age():
    data = json.load(request.json)
    df = pd.read_json(data, lines=True)
    df.visits = df.visits.apply(list2domain)
    res = model.predict(df["visits"])
    print(type(res))
    print(res)


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')