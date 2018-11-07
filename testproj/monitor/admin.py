from django.contrib import admin
from .models import Document, DataSet, Courses, Platform, Sections, Subsections, Pages


@admin.register(Document)
class DocumentAdmin(admin.ModelAdmin):
    search_fields = ("document", "uploaded_at")
    list_display = ("document", "uploaded_at")


@admin.register(DataSet)
class DataSetAdmin(admin.ModelAdmin):
    list_display = ("spark_count",)


@admin.register(Courses)
class CoursesAdmin(admin.ModelAdmin):
    list_display = ("name", "session")


@admin.register(Platform)
class PlatformAdmin(admin.ModelAdmin):
    list_display = ("name", "url")


@admin.register(Sections)
class SectionsAdmin(admin.ModelAdmin):
    list_display = ("global_id", "name")


@admin.register(Subsections)
class SubsectionsAdmin(admin.ModelAdmin):
    list_display = ("name", "section")


@admin.register(Pages)
class PagesAdmin(admin.ModelAdmin):
    list_display = ("name", "subsection")