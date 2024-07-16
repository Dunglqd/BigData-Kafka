
import sys  
import os  
import argparse  
import ujson as json 
import socket 
import re  
import pandas as pd
from termcolor import cprint  

from pyspark import SparkContext  
from pyspark.streaming import StreamingContext  
from pyspark.streaming.kafka import KafkaUtils 
from pyspark.sql import SparkSession

from kafka import KafkaProducer 
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier, LinearSVC
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
df = pd.read_csv('D:\Code\Personal\BD_F\dataset_sdn.csv')
def send_to_kafka(data, producer, topic):
 
    producer.send(topic, str(data))


def print_and_send(rdd, producer, topic):
    
    spark = SparkSession.builder.appName("SparkSQLApp").getOrCreate()

    db = spark.createDataFrame(rdd, ["host", "stats"])

    db.createOrReplaceTempView("ddos_stats")

    results_df = spark.sql("""
        SELECT host,
               stats[0][0] / stats[0][1] as shortratio,
               stats[1][0] / stats[1][1] as longratio,
               stats[0][2] as attackers
        FROM ddos_stats
    """)

    results = ""
    for row in results_df.collect():
        new_entry = {"@type": "detection.ddos",
                     "dst_ip": row.host,
                     "shortratio": row.shortratio,
                     "longratio": row.longratio,
                     "attackers": row.attackers}

        results += ("%s\n" % json.dumps(new_entry))

    cprint(results)
    send_to_kafka(results, producer, topic)


def inspect_ddos(stream_data):
    local_ip_pattern = re.compile(network_filter)
    filtered_stream_data = stream_data \
        .map(lambda x: json.loads(x[1])) \
        .filter(lambda json_rdd: ("ipfix.sourceIPv4Address" in json_rdd.keys() and
                                  "ipfix.destinationIPv4Address" in json_rdd.keys()
                                  ))
    small_window = filtered_stream_data.window(base_window_length, base_window_length)

    incoming_small_flows_stats = small_window \
        .filter(lambda json_rdd: re.match(local_ip_pattern, json_rdd["ipfix.destinationIPv4Address"])) \
        .map(lambda json_rdd: (json_rdd["ipfix.destinationIPv4Address"],
                               (json_rdd["ipfix.packetDeltaCount"], 0, {json_rdd["ipfix.sourceIPv4Address"]})))
    outgoing_small_flows_stats = small_window \
        .filter(lambda json_rdd: re.match(local_ip_pattern, json_rdd["ipfix.sourceIPv4Address"])) \
        .map(lambda json_rdd: (json_rdd["ipfix.sourceIPv4Address"],
                               (0, json_rdd["ipfix.packetDeltaCount"], set()))) \

    small_window_aggregated = incoming_small_flows_stats.union(outgoing_small_flows_stats)\
        .reduceByKey(lambda actual, update: (actual[0] + update[0],
                                             actual[1] + update[1],
                                             actual[2].union(update[2])))

    union_long_flows = small_window_aggregated.window(long_window_length, base_window_length)
    long_window_aggregated = union_long_flows.reduceByKey(lambda actual, update: (actual[0] + update[0],
                                                          actual[1] + update[1])
                                                          )

    windows_union = small_window_aggregated.join(long_window_aggregated)

    nonzero_union = windows_union.filter(lambda rdd: rdd[1][0][1] != 0 and rdd[1][1][1] != 0)

    windows_union_filtered = nonzero_union.filter(lambda rdd: rdd[1][0][0] > minimal_incoming and
                                                  float(rdd[1][0][0]) / rdd[1][0][1] > float(rdd[1][1][0]) /
                                                  rdd[1][1][1] * threshold
                                                  )
    return windows_union_filtered

def run_spark_ml(df):

    (train_data, test_data) = df.randomSplit([0.8, 0.2], seed=1234)

    assembler = VectorAssembler(inputCols=['shortratio', 'longratio'], outputCol='features')

    train_data = assembler.transform(train_data)
    test_data = assembler.transform(test_data)

    dt = DecisionTreeClassifier(labelCol="label", featuresCol="features")
    dt_model = dt.fit(train_data)

    svm = LinearSVC(maxIter=10, regParam=0.1)
    svm_model = svm.fit(train_data)

    layers = [2, 5, 4, 3]
    dnn = MultilayerPerceptronClassifier(layers=layers, seed=1234)
    dnn_model = dnn.fit(train_data)

    dt_predictions = dt_model.transform(test_data)
    svm_predictions = svm_model.transform(test_data)
    dnn_predictions = dnn_model.transform(test_data)

    evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    dt_accuracy = evaluator.evaluate(dt_predictions)
    svm_accuracy = evaluator.evaluate(svm_predictions)
    dnn_accuracy = evaluator.evaluate(dnn_predictions)

    print("Decision Tree Accuracy:", dt_accuracy)
    print("SVM Accuracy:", svm_accuracy)
    print("DNN Accuracy:", dnn_accuracy)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-iz", "--input_zookeeper", help="input zookeeper hostname:port", type=str, required=True)
    parser.add_argument("-it", "--input_topic", help="input kafka topic", type=str, required=True)
    parser.add_argument("-oz", "--output_zookeeper", help="output zookeeper hostname:port", type=str, required=True)
    parser.add_argument("-ot", "--output_topic", help="output kafka topic", type=str, required=True)
    parser.add_argument("-nf", "--network_filter", help="regular expression filtering the watched IPs", type=str, required=True)

    args = parser.parse_args()

    application_name = os.path.basename(sys.argv[0])  
    kafka_partitions = 1  

    threshold = 50  
    minimal_incoming = 100000  
    long_window_length = 7200  
    base_window_length = 30  

    network_filter = args.network_filter  

    sc = SparkContext(appName=application_name + " " + " ".join(sys.argv[1:]))  
    ssc = StreamingContext(sc, 1)  
    spark = SparkSession.builder.appName(application_name).getOrCreate()
    kafka_stream = KafkaUtils.createDirectStream(ssc, [input_topic], {"metadata.broker.list": input_zookeeper})
    processed_stream = kafka_stream.map(lambda x: json.loads(x[1])) \
                                   .filter(lambda json_rdd: ("ipfix.sourceIPv4Address" in json_rdd.keys() and
                                                             "ipfix.destinationIPv4Address" in json_rdd.keys())) \
                                   .window(base_window_length, base_window_length) \
                                   .foreachRDD(process_stream_data)
    input_stream = KafkaUtils.createStream(ssc, args.input_zookeeper, "spark-consumer-" + application_name,
                                           {args.input_topic: kafka_partitions})

    ddos_result = inspect_ddos(input_stream)

    kafka_producer = KafkaProducer(bootstrap_servers=args.output_zookeeper,
                                   client_id="spark-producer-" + application_name)

    ddos_result.foreachRDD(lambda rdd: print_and_send(rdd, kafka_producer, args.output_topic))

    kafka_producer.flush()
    run_spark_ml(df)

    ssc.start()
    ssc.awaitTermination()
