import json
from django.shortcuts import render, redirect
import logging

#import pandas as pd

from datetime import datetime 

from .forms import DocumentForm
from .models import *
from django.shortcuts import get_object_or_404
from django.core.files.uploadedfile import UploadedFile
#import pickle

from .tasks import handle_spark
from django.http import JsonResponse, HttpResponse

from collections import OrderedDict
from .fusioncharts import FusionCharts


logger = logging.getLogger('cel_logging')

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


def data(request):
    if request.method == "POST":
        handle_spark.delay()
        return render(request, "data.html")
    elif request.method == "GET":
        ds = DataSet.objects.all().first()
        return render(request, "data.html", {"count": ds.spark_count})


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
    dataSource = {}
    dataSource['chart'] = { 
        "caption": "Customer Happiness by Response time",
        "yaxisname": "Response Time(in secodns)",
        "xaxisname": "Count pages",
        "yaxismaxvalue": "10",
        "xaxisminvalue": "0",
        "xaxismaxvalue": "100",
        "theme": "candy",
        "plottooltext": "<div id='valueDiv'>Page numder : <b>$seriesName</b><br> Time : <b>$yDataValue sec</b></div>"
    }

    with open('static/data.json') as f:
        dataJson = json.loads(f.read())
    #json_data = open('static/data.json')
    #dataJson = json.loads(json_data)
    #logger.info(dataJson)

    #json_data.close()

    dataSource["data"] = []
    
    # Convert the data in the `chartData` array into a format that can be consumed by FusionCharts. 
    # The data for the chart should be in an array wherein each element of the array is a JSON object
    # having the `label` and `value` as keys.

    # Iterate through the data in `chartData` and insert in to the `dataSource['data']` list.
    for key, value in chartData.items():
        data = {}
        data["xasix"] = key
        data["yasix"] = value
        dataSource["data"].append(data)


    # Create an object for the column 2D chart using the FusionCharts class constructor
    # The chart data is passed to the `dataSource` parameter.
    zoomscatter = FusionCharts("zoomscatter", "ex1" , "900", "750", "chart-container", "json", dataSource)
    logger.info(str(dataSource))

    return render(request, 'dashboard.html', {'output' : zoomscatter.render(), 'chartTitle': 'Labor Intensity'})