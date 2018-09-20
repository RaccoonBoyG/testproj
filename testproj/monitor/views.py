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
from django.http import JsonResponse


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
        ds = DataSet.objects.all().latest()
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
    return render(request, 'base.html')