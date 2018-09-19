import logging
import datetime

from testproj.celery import app
from .models import Document
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

logger = logging.getLogger('celery_logging')


@app.task(bind=True)
def handle_spark(self, *args):
    # logger.info("Course Начали: {0}".format(strftime("%Y-%m-%d %H:%M:%S", gmtime())))
        def el_in_line(line, els):
            b = []
            for el in els:
                b.append(el in line)
            return not any(b)

        def filter_log(line):  
                return el_in_line(line, ['/container/','Drupal','/instructor','{"username": ""','/info','edx.ui.lms.link_clicked','/jump_to','/progress','seek_video','play_video','pause_video','load_video','/xblock/','/xmodule/','edx.ui.lms.sequence.next_selected','stop_video','seq_goto','seq_next','problem_graded','speed_change_video','problem_check','/course/','edx.ui.lms.sequence.previous_selected','/masquerade','studio.lektorium.tv'])


        conf = SparkConf().setAppName('TestProjApp1')
        sc = SparkContext.getOrCreate(conf=conf)
        sql_sc = SQLContext(sc)
        logRDD = sc.textFile("/home/alex/big_data_edx/testproj/testproj/uploads/uploads/TPUIN2017_05.log.gz")
        logger.info(logRDD)
        logRDD = logRDD.map(lambda line: line.split('{', 1)[1])
        char_elem = '{'
        logRDD = logRDD.map(lambda line: f'{char_elem}{line}')
        log = logRDD.filter(filter_log)
        log = log.first()
        logger.info(log)