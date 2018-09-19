from django.contrib import admin
from .models import Document, DataSet


@admin.register(Document)
class DocumentAdmin(admin.ModelAdmin):
    search_fields = ("document", "uploaded_at")
    list_display = ("description","document", "uploaded_at")


@admin.register(DataSet)
class DataSetAdmin(admin.ModelAdmin):
    list_display = ("spark_count",)

