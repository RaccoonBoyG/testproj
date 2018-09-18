from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql import SparkSession ,Row
from pyspark.streaming import StreamingContext

#conf = SparkConf().setAppName('TestProjApp')
#sc = SparkContext.getOrCreate(conf=conf)


def el_in_line(line, els):
    b = []
    for el in els:
        b.append(el in line)
    return not any(b)


def filter_log(line):  
        return el_in_line(line, ['/container/','Drupal','/instructor','{"username": ""','/info','edx.ui.lms.link_clicked','/jump_to','/progress','seek_video','play_video','pause_video','load_video','/xblock/','/xmodule/','edx.ui.lms.sequence.next_selected','stop_video','seq_goto','seq_next','problem_graded','speed_change_video','problem_check','/course/','edx.ui.lms.sequence.previous_selected','/masquerade','studio.lektorium.tv'])


# def upload_from_spark(sc,sql_sc):
#     logRDD = sc.textFile("testproj/uploads/uploads/*.gz")
#     logRDD = logRDD.map(lambda line: line.split('{', 1)[1])
#     char_elem = '{'
#     logRDD = logRDD.map(lambda line: f'{char_elem}{line}')
#     log = logRDD.filter(filter_log)
#     df_log = sql_sc.read.json(log).persist()
#     df_log = df_log[['username','time','event_type','page']]
#     new_column = F.when(df_log.event_type!='page_close', F.split('event_type','/')[5]).when(df_log.event_type=='page_close',F.split('page','/')[7]).otherwise('page_close')
#     df_log_test = df_log.withColumn('event_type', new_column)
#     df_log_test = df_log_test.filter(df_log_test.event_type != '')
#     df_log_test1 = df_log_test.withColumn("id",F.monotonically_increasing_id())
#     mydict = df_log_test1.toPandas().set_index('id').T.to_dict('list')
#     pickle.dump(mydict, open("/tmp/mydict", "wb"))


def process(time,rdd):
    print("========= %s =========" % str(time))
    # Get the singleton instance of SparkSession
    conf = SparkConf().setAppName('BuffferApp')
    spark = SparkContext.getOrCreate(conf=conf)
    print(rdd)
    # Convert RDD[String] to RDD[Row] to DataFrame
    if not rdd.isEmpty(): 
        log = rdd.map(lambda line: line.split('{', 1)[1])
        print(type(log))
        print("1111111!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        char_elem = '{'
        log = log.map(lambda line: f'{char_elem}{line}')
        log = log.filter(filter_log)
        print(type(log))
        print(log.first())
        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        wordsDataFrame = spark.createDataFrame(log)
        wordsDataFrame.show()
    else:
        print('ELSE')        


if __name__ == "__main__":
    #spark = SparkSession.builder.master("local").appName("TestProjApp").getOrCreate()
    conf = SparkConf().setAppName('TestProjApp')
    #sc = SparkContext.getOrCreate(conf=conf)
    sc = SparkContext.getOrCreate(conf=conf)
    ssc = StreamingContext(sc, 1)
    ssc.checkpoint("/tmp/spark")
    logRDD = ssc.textFileStream("testproj/uploads/uploads/*.gz")


    logRDD.pprint()

    logRDD.foreachRDD(process)
    ssc.start()
    ssc.awaitTermination()
    #mydict = df_log_test1.toPandas().set_index('id').T.to_dict('list')
    #pickle.dump(mydict, open("/tmp/mydict", "wb"))
