import json
from django.shortcuts import render, redirect
import logging

#import pandas as pd


from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from datetime import datetime

from .forms import DocumentForm
from .models import Document
from django.shortcuts import get_object_or_404
from django.core.files.uploadedfile import UploadedFile
#import pickle

from pyspark import SparkContext, SparkConf



logger = logging.getLogger('cel_logging')

def el_in_line(line, els):
    b = []
    for el in els:
        b.append(el in line)
    return not any(b)

def filter_log(line):  
        return el_in_line(line, ['/container/','Drupal','/instructor','{"username": ""','/info','edx.ui.lms.link_clicked','/jump_to','/progress','seek_video','play_video','pause_video','load_video','/xblock/','/xmodule/','edx.ui.lms.sequence.next_selected','stop_video','seq_goto','seq_next','problem_graded','speed_change_video','problem_check','/course/','edx.ui.lms.sequence.previous_selected','/masquerade','studio.lektorium.tv'])

def filter_csv(line):
    if(("unenrolled") not in line):
        return line

def calculateTime(df_log):
    time_open = {}
    user_obj = {}
    time_all = []
    for row in df_log.take(50):
        if row.page==None:
            time_last_open = row.time.split('.',1)[0]
            time_last_open1 = datetime.strptime(time_last_open, "%Y-%m-%dT%H:%M:%S")
            time_open[row.event_type]=time_last_open
            user_obj[row.username] = time_open
    time_all.append(user_obj)
    return time_all


def upload_from_spark(request):
    conf = SparkConf().setAppName('TestProjApp1')
    sc = SparkContext.getOrCreate(conf=conf)
    sql_sc = SQLContext(sc)
    logRDD = sc.textFile("uploads/uploads/*.gz")
    logger.info(logRDD)
    logRDD = logRDD.map(lambda line: line.split('{', 1)[1])
    char_elem = '{'
    logRDD = logRDD.map(lambda line: f'{char_elem}{line}')
    log = logRDD.filter(filter_log)
    log = log.first()
    logger.info(log)
    # df_log = sql_sc.read.json(log).persist()
    # df_log = df_log[['username','time','event_type','page']]
    # new_column = F.when(df_log.event_type!='page_close', F.split('event_type','/')[5]).when(df_log.event_type=='page_close',F.split('page','/')[7]).otherwise('page_close')
    # df_log_test = df_log.withColumn('event_type', new_column)
    # df_log_test = df_log_test.filter(df_log_test.event_type != '')
    # df_log_test1 = df_log_test.withColumn("id",F.monotonically_increasing_id())
    # mydict = df_log_test1.toPandas().set_index('id').T.to_dict('list')
    # pickle.dump(mydict, open("/tmp/mydict", "wb"))
    context = {
        'first_obj': log,
    }
    return render(request, 'upload_from_spark.html', context)    


def upload_file(request):
    documents = Document.objects.all()
    if request.method == 'POST':
        form = DocumentForm(request.POST, request.FILES)
        if form.is_valid():
            form.save()
        return redirect('/upload')
    else:
        form = DocumentForm()
    return render(request, 'upload_file.html', {
        'form': form,
        'documents': documents
    })    


def delete(request):
    if request.method != 'POST':
        raise HTTP404

    docId = request.POST.get('document', None)
    docToDel = get_object_or_404(Document, pk = docId)
    docToDel.document.delete()
    docToDel.delete()

    return redirect('/upload')


def page_view(request):
    return render(request, 'base.html')