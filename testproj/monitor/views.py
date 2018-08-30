from django.shortcuts import render
import json
from django.shortcuts import render, redirect

from pyspark import SparkContext, SparkConf
import logging
import json

logger = logging.getLogger('cel_logging')

def normlizejson(log):
    words = []
    stufflist = list()
    if(type(log) == list()):
        for line in log:
            j = json.loads(line)
            words = j
            stufflist.append(words)
        return stufflist
    elif(type(log) == str()):
        j = json.loads(log)
        words = j
        stufflist.append(words)
        return stufflist



def upload_from_json(request):
    conf = SparkConf().setAppName('TestProjApp')
    sc = SparkContext.getOrCreate(conf=conf)
    logRDD = sc.textFile("/home/alex/big_data_edx/tracking.log")
    test = logRDD.filter(lambda line: "username" in line)
    test = normlizejson(test.first())
    logger.info(test)
    test2 = logRDD.count()
    context = {
        'first_obj': test,
        'second_obj':test2,
    }
    return render(request, 'upload_from_json.html', context)