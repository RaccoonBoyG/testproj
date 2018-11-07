import json
from django.shortcuts import render, redirect
import logging

#import pandas as pd

from datetime import datetime 

from .forms import DocumentForm
from .models import Document, DataSet
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
    dataSource = OrderedDict()

    # The `chartConfig` dict contains key-value pairs data for chart attribute
    chartConfig = OrderedDict()
    chartConfig["caption"] = "Labor Intensity"
    #chartConfig["subCaption"] = "In MMbbl = One Million barrels"
    chartConfig["xAxisName"] = "Razdel 1"
    chartConfig["yAxisName"] = "Time(second)"
    chartConfig["numberSuffix"] = " Sec"
    chartConfig["theme"] = "candy"

    # The `chartData` dict contains key-value pairs data
    chartData = OrderedDict()
    chartData["Venezuela"] = 290
    chartData["Saudi"] = 260
    chartData["Canada"] = 180
    chartData["Iran"] = 140
    chartData["Russia"] = 115
    chartData["UAE"] = 100
    chartData["US"] = 30
    chartData["China"] = 30

    json_data = open('static/data.json')
    dataJson = json.loads(json_data)
    logger.info(dataJson)

    json_data.close()


    dataSource["chart"] = chartConfig
    dataSource["data"] = []
    
    # Convert the data in the `chartData` array into a format that can be consumed by FusionCharts. 
    # The data for the chart should be in an array wherein each element of the array is a JSON object
    # having the `label` and `value` as keys.

    # Iterate through the data in `chartData` and insert in to the `dataSource['data']` list.
    for key, value in chartData.items():
        data = {}
        data["label"] = key
        data["value"] = value
        dataSource["data"].append(data)


    # Create an object for the column 2D chart using the FusionCharts class constructor
    # The chart data is passed to the `dataSource` parameter.
    column2D = FusionCharts("column2d", "ex1" , "1000", "800", "chart-1", "json", dataSource)

    return render(request, 'dashboard.html', {'output' : column2D.render(), 'chartTitle': 'Simple Chart Using Array'})