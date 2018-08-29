from django.shortcuts import render
import json
from django.shortcuts import render, redirect

from pyspark import SparkContext, SparkConf
import logging

logger = logging.getLogger('cel_logging')

def upload_from_json(request):
    conf = SparkConf().setAppName('TestProjApp')
    sc = SparkContext(conf=conf)
    logRDD = sc.textFile("/home/alex/big_data_edx/tracking.log")
    test = logRDD.take(5)
    context = {
        'first_obj': test,
    }
    return render(request, 'upload_from_json.html', context)