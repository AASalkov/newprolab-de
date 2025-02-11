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
    js = json.loads(request.json)
    df = pd.DataFrame(js)
    df.visits = df.visits.apply(list2domain)
    df["gender_age"] = model.predict(df["visits"])
    res = ""
    for index, row in df.iterrows():
        res = res + '{"uid": "' + row['uid'] + '", "gender_age": "' + row['gender_age'] + '"}' + "\n"
    print(res)
    return res


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')