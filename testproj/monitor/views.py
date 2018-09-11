from django.shortcuts import render
import json
from django.shortcuts import render, redirect
import logging
#import pandas as pd

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from datetime import datetime

from .forms import DocumentForm
from .models import Document


logger = logging.getLogger('cel_logging')

def el_in_line(line, els):
    b = []
    for el in els:
        b.append(el in line)
    return not any(b)

def filter_log(line):  
        return el_in_line(line, ['/container/','Drupal','/instructor','{"username": ""','/info','edx.ui.lms.link_clicked','/jump_to','/progress','seek_video','play_video','pause_video','load_video','/xblock/','/xmodule/','edx.ui.lms.sequence.next_selected','stop_video','seq_goto','seq_next','problem_graded','speed_change_video','problem_check','/course/','edx.ui.lms.sequence.previous_selected','/masquerade'])

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


def upload_from_json_spark():
    conf = SparkConf().setAppName('TestProjApp')
    sc = SparkContext.getOrCreate(conf=conf)
    sql_sc = SQLContext(sc)
    logRDD = sc.textFile("/home/alex/big_data_edx/track/TPU+IN+2017_05.log.gz")
    logRDD = logRDD.map(lambda line: line.split('{', 1)[1])
    char_elem = '{'
    logRDD = logRDD.map(lambda line: f'{char_elem}{line}')
    log = logRDD.filter(filter_log)
    df_log = sql_sc.read.json(log)
    df_log = df_log[['username','time','event_type','page']]
    new_column = F.when(df_log.event_type!='page_close', F.split('event_type','/')[5]).when(df_log.event_type=='page_close',F.split('page','/')[7]).otherwise('page_close')
    df_log_test = df_log.withColumn('event_type', new_column)
    df_log_test = df_log_test.filter(df_log_test.event_type != '')
#new_column1 = F.when(df_log.event_type=='page_close',F.split('page','/')[7])

#csvRDD = sc.textFile('/home/alex/big_data_edx/track/TPU_IN_2017_05_grade_report_2018-08-31-1106.csv')
#csv = csvRDD.filter(filter_csv)
#df_csv = sql_sc.read.csv(csv)
#result_df = workDataFrame(df_log,df_csv).persist()
#test_start = result_df[['username','time','event_type']]
#test_start = test_start.withColumn('next_time', F.lead(test_start['time']).over(Window.partitionBy("username").orderBy('time')))
#timeFmt = "yyyy-MM-dd'T'HH:mm:ss"
#timeDiff = (F.unix_timestamp('time', format=timeFmt)-F.unix_timestamp('prev_time', format=timeFmt))
#test_start = test_start.withColumn("Duration", timeDiff)

    #log = normlizejson(log.first())
    #countLog = logRDD.count()
    #return render(request, 'upload_from_json.html')

def upload_from_json(request):
    documents = Document.objects.all()
    if request.method == 'POST':
        form = DocumentForm(request.POST, request.FILES)
        if form.is_valid():
            form.save()
            return redirect('/upload_json')
    else:
        form = DocumentForm()
    return render(request, 'upload_from_json.html', {
        'form': form,
        'documents': documents
    })    

def page_view(request):
    return render(request, 'base.html')