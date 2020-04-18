from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.sql.types import *
from pyspark.sql.functions import *

## Конфиги
kafka_bootstrap = "bda1.kafkatst.marathon.mesos.sportmaster.ru:9092"
topic_in = "clubpro_test"

spark = SparkSession.builder.appName("ClubPro").getOrCreate()
spark.sparkContext.setLogLevel('WARN')

schema = StructType(
   fields = [
      StructField("code", LongType(), True),
      StructField("client_reg_code", LongType(), True),
      StructField("action_code", IntegerType(), True),
      StructField("comm_type", IntegerType(), True),
      StructField("comm_ident", LongType(), True),
      StructField("comm_sender", StringType(), True),
      StructField("create_type", IntegerType(), True),
      StructField("dat", StringType(), True),
      StructField("dat_send", StringType(), True),
      StructField("dat_accept",StringType(), True),
      StructField("state", IntegerType(), True),
      StructField("group_type",IntegerType(), True),
      StructField("is_high_priority", IntegerType(), True),
      StructField("event_type", IntegerType(), True),
      StructField("dat_expire", StringType(), True),
      StructField("dat_state", StringType(), True),
	  StructField("params", StringType(), True)
])

## Считываем и распаковываем json-сообщения
st = \
    spark \
        .read.format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap) \
        .option("subscribe", topic_in) \
        .option("startingOffsets", "earliest") \
		.option("kafka.group.id", "ASalkov") \
        .load() \
        .selectExpr("CAST(value as string)")\
        .select(from_json("value", schema).alias("value"))\
        .select(col("value.*"))\

st.write.mode("overwrite").json("/tmp/results.json")
  
to_console = \                                                                                                                                                                                                     
    st \                                                                                                                                                                                                           
        .selectExpr("CAST(value as string)")\                                                                                                                                                                      
        .select(from_json("value", schema).alias("value"))\                                                                                                                                                        
        .select(col("value.*"))\                                                                                                                                                                                   
        .writeStream.format("console") \                                                                                                                                                                           
        .option("checkpointLocation", "chk_2") \                                                                                                                                                                   
        .option("truncate", "false") \                                                                                                                                                                             
        .start()   
		
		
		
		
		
		
		
		
		
		
		
		
		
from pyspark.sql import SparkSession                                                                                                                                                                               
from pyspark.streaming import StreamingContext                                                                                                                                                                     
from pyspark.sql.types import *                                                                                                                                                                                    
from pyspark.sql.functions import *                                                                                                                                                                                
                                                                                                                                                                                                                   
kafka_bootstrap = "bda1.kafkatst.marathon.mesos.sportmaster.ru:9092"                                                                                                                                               
topic_in = "clubpro_test"                                                                                                                                                                                          
                                                                                                                                                                                                                   
spark = SparkSession.builder.appName("ClubPro").getOrCreate()                                                                                                                                                      
spark.sparkContext.setLogLevel('WARN')                                                                                                                                                                             
                                                                                                                                                                                                                   
schema = StructType(                                                                                                                                                                                               
   fields = [                                                                                                                                                                                                      
      StructField("CODE", LongType(), True),                                                                                                                                                                       
      StructField("CLIENT_REG_CODE", LongType(), True),                                                                                                                                                            
      StructField("ACTION_CODE", IntegerType(), True),                                                                                                                                                             
      StructField("COMM_TYPE", IntegerType(), True),                                                                                                                                                               
      StructField("COMM_IDENT", LongType(), True),                                                                                                                                                                 
      StructField("COMM_SENDER", StringType(), True),                                                                                                                                                              
      StructField("CREATE_TYPE", IntegerType(), True),                                                                                                                                                             
      StructField("DAT", StringType(), True),                                                                                                                                                                      
      StructField("DAT_SEND", StringType(), True),                                                                                                                                                                 
      StructField("DAT_ACCEPT",StringType(), True),                                                                                                                                                                
      StructField("STATE", IntegerType(), True),                                                                                                                                                                   
      StructField("GROUP_TYPE",IntegerType(), True),                                                                                                                                                               
      StructField("IS_HIGH_PRIORITY", IntegerType(), True),                                                                                                                                                        
      StructField("EVENT_TYPE", IntegerType(), True),                                                                                                                                                              
      StructField("DAT_EXPIRE", StringType(), True),                                                                                                                                                               
      StructField("DAT_STATE", StringType(), True),                                                                                                                                                                
      StructField("PARAMS", StringType(), True)                                                                                                                                                                    
])                                                                                                                                                                                                                 
                                                                                                                                                                                                                   
st = \                                                                                                                                                                                                             
    spark \                                                                                                                                                                                                        
        .read.format("kafka") \                                                                                                                                                                                    
        .option("kafka.bootstrap.servers", kafka_bootstrap) \                                                                                                                                                      
        .option("subscribe", topic_in) \                                                                                                                                                                           
        .option("startingOffsets", "earliest") \                                                                                                                                                                   
        .load() \                                                                                                                                                                                                  
                                                                                                                                                                                                                   
                                                                                                                                                                                                                   
st.selectExpr("CAST(value as string)").select(from_json("value", schema).alias("value")).show(5, False)  		