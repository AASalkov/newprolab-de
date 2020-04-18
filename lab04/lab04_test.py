from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.types import *
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import from_json
import json
from pyspark.sql import Window
from pyspark.sql.functions import *
import re
from urllib.parse import urlparse
from urllib.request import urlretrieve, unquote
from pyspark.sql.functions import udf
from pyspark.ml import PipelineModel
from pyspark.ml.feature import IndexToString

topic_in = "alexander_salkov_lab04_in"
topic_out = "alexander_salkov_lab04_out"
kafka_bootstrap = "10.1.31.10:6667"

schema = StructType([
    StructField("uid", StringType(), True),
    StructField("visits", ArrayType(StructType([
                            StructField("url", StringType(), False),
                            StructField("timestamp", LongType(), False)
                          ])
                     ), True)
])

spark = SparkSession.builder.appName("SimpleStreamingApp").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

st = spark \
  .readStream \
  .format("kafka") \
  .option("checkpointLocation", "/tmp/checkpoint-read") \
  .option("kafka.bootstrap.servers", kafka_bootstrap) \
  .option("subscribe", topic_in) \
  .option("startingOffsets", "latest") \
  .load() \
  .selectExpr("CAST(value as string)") \
  .select(from_json("value", schema).alias("value")) \
  .select(col("value.*")) \

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
	

udfFunc = udf(lambda y: array2domain(y), ArrayType(StringType()))

domains_df = st.select('uid', udfFunc('visits').alias("urls")) 


model = PipelineModel.load("lab04/model")
indexed  = model.transform(domains_df)
labels = model.stages[1].labels	


converter = IndexToString(inputCol="prediction", outputCol="gender_age", labels = labels)
converted = converter.transform(indexed)

out_df = converted.select("uid", "gender_age")

out_columns = list(out_df.columns)

query = out_df \
  .select(to_json(struct(*out_columns)).alias("value")) \
  .writeStream \
  .outputMode("update") \
  .format("kafka") \
  .option("checkpointLocation", "chk_12") \
  .option("kafka.bootstrap.servers", kafka_bootstrap ) \
  .option("topic", topic_out) \
  .start()
  
query.awaitTermination()  