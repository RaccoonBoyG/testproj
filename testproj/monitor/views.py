from django.shortcuts import render
import json
from django.shortcuts import render, redirect
import logging
#import pandas as pd

from pyspark import SparkContext, SparkConf
#from pyspark.sql import SQLContext

logger = logging.getLogger('cel_logging')

def normlizejson(log):
    words = []
    stufflist = list()
    if(type(log) is list):
        for line in log:
            j = json.loads(line)
            words = j
            stufflist.append(words)
        return stufflist
    elif(type(log) is str):
        j = json.loads(log)
        words = j
        stufflist.append(words)
        return stufflist


def upload_from_json(request):
    conf = SparkConf().setAppName('TestProjApp')
    sc = SparkContext.getOrCreate(conf=conf)
    #sql_sc = SQLContext(sc)
    csvRDD = sc.textFile('/home/alex/big_data_edx/track/UrFU_IHA.b.Hu-0063.1_spring_2018_problem_grade_report_2018-08-30-1000.csv').map(lambda line: line.split(","))
    #s_df = sql_sc.createDataFrame(pandas_df)
    logRDD = sc.textFile("/home/alex/big_data_edx/track/tracking.log")
    csv = csvRDD.collect()
    log = logRDD.filter(lambda line: "alexKekovich" in line)
    log = normlizejson(log.first())
    countLog = logRDD.count()
    context = {
        'csv': csv,
        'log': log,
        'countLog':countLog,
    }
    return render(request, 'upload_from_json.html', context)