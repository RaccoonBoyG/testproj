from django.contrib import admin
from .models import Document


@admin.register(Document)
class DocumentAdmin(admin.ModelAdmin):
    search_fields = ("document", "uploaded_at")
    list_display = ("description","document", "uploaded_at")


