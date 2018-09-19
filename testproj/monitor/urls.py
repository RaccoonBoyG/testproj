from django.urls import path ,re_path
from .views import upload_file, page_view, delete, data
"""Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
"""
urlpatterns = [
    re_path(r'^$',page_view, name='page_view'),
    re_path(r'^upload/', upload_file, name='upload_file'),
    re_path(r'^delete', delete, name='delete'),
    re_path(r'^data', data, name='data')
]