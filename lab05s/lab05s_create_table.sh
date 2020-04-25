cat <<END | clickhouse-client --port 9090 --multiline
CREATE TABLE alexander_salkov_lab05s
(
  uid  		 String,
  gender_age String,
  visits			  String
)
ENGINE = Kafka()
SETTINGS
kafka_broker_list = '10.1.31.10:6667',
kafka_topic_list = 'alexander_salkov_lab05s_out',
kafka_group_name = 'tut_kafka',
kafka_format = 'JSONEachRow'
END