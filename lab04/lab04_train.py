import re
from urllib.parse import urlparse
from urllib.request import urlretrieve, unquote
from pyspark.sql.types import  StringType, ArrayType, StructType, StructField, LongType
from pyspark.sql.functions import udf
from pyspark.ml import Pipeline
from pyspark.ml.feature import CountVectorizer
from pyspark.ml.feature import StringIndexer, IndexToString
from pyspark.ml.classification import LogisticRegression

def url2domain(url):
    url = re.sub('(http(s)*://)+', 'http://', url)
    parsed_url = urlparse(unquote(url.strip()))
    if parsed_url.scheme not in ['http','https']: return None
    netloc = re.search("(?:www\.)?(.*)", parsed_url.netloc).group(1)
    if netloc is not None: return str(netloc.encode('utf8')).strip()
    return None

def array2domain(x):
    result = []
    for rec in x:
        result.append(url2domain(rec[0]))
    return result
	
# DataFrame
schema = StructType([
    StructField("uid", StringType(), True),
    StructField("gender_age", StringType(), True),
    StructField("visits", ArrayType(StructType([
                            StructField("url", StringType(), False),
                            StructField("timestamp", LongType(), False)
                          ])
                     ), True)
])	


spark = SparkSession.builder.appName("Spark ML").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

udfFunc = udf(lambda y: array2domain(y), ArrayType(StringType()))

df = spark.read.json('lab04/lab04_train_merged_labels.json', schema = schema) \
.select('uid', udfFunc('visits').alias("urls"), 'gender_age') 


# Model
cv = CountVectorizer(inputCol="urls", outputCol="features")
indexer = StringIndexer(inputCol="gender_age", outputCol="label")
lr = LogisticRegression()
pipeline = Pipeline(stages=[cv, indexer, lr])

pipeline.fit(df)
pipeline.save("lab04/model")