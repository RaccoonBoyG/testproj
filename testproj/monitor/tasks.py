import logging
import datetime

from testproj.celery import app
from .models import Document

logger = logging.getLogger('celery_logging')


@app.task(bind=True)
def handle_spark(self, *args):
    # logger.info("Course Начали: {0}".format(strftime("%Y-%m-%d %H:%M:%S", gmtime())))
    Document.upload_from_spark()