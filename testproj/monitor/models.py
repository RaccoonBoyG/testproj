from django.db import models

class Document1(models.Model):
    description = models.CharField(max_length=255, blank=True, null=True)



class Document(models.Model):
    description = models.CharField(max_length=255, blank=True, null=True)
    document = models.FileField(upload_to='uploads/')
    uploaded_at = models.DateTimeField(auto_now_add=True)
