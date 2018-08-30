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

def hasFilter(log):



def upload_from_json(request):
    conf = SparkConf().setAppName('TestProjApp')
    sc = SparkContext.getOrCreate(conf=conf)
    logRDD = sc.textFile("/home/alex/big_data_edx/track/tracking.log")
    test = logRDD.filter(hasFilter)
    test = normlizejson(test.first())
    test2 = logRDD.count()
    context = {
        'first_obj': test,
        'second_obj':test2,
    }
    return render(request, 'upload_from_json.html', context)