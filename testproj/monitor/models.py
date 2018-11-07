from django.db import models
import logging

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F

logger = logging.getLogger('cel_logging')


class Document(models.Model):
    name = models.CharField("Наименование курса",max_length=255, blank=True, null=True)
    document = models.FileField(upload_to='uploads/')
    uploaded_at = models.DateTimeField(auto_now_add=True)

class Courses(models.Model):
    name = models.CharField("Наименование курса",max_length=1024, blank=True, null=True)
    session = models.CharField("Номер запуска курса",max_length=512, blank=True, null=True)
    section = models.ManyToManyField('Sections', verbose_name="Разделы курса", blank=True,
                                    null=True)
    subsection = models.ManyToManyField('Subsections', verbose_name="Подразделы курса", blank=True,
                                    null=True)
    partner = models.ForeignKey('Platform',on_delete=CASCADE, verbose_name="Платформа", null=True)

    class Meta:
        verbose_name = 'Курс'
        verbose_name_plural = 'Курсы'

class Platform(models.Model):
    name = models.CharField("Наименование", blank=True, null=True, max_length=1024)
    url = models.CharField("Ссылка на сайт платформы", blank=True, null=True, max_length=512)
    image = models.CharField("Изображение платформы", blank=True, null=True, max_length=512)

    class Meta:
        verbose_name = 'платформа'
        verbose_name_plural = 'платформы'

class Sections(models.Model):
    global_id = models.CharField("ИД раздела",max_length=1024, blank=True, null=True)
    name = models.CharField("Наименование",max_length=1024, blank=True, null=True)

    class Meta:
        verbose_name = 'Раздел'
        verbose_name_plural = 'Разделы'
    
class Subsections(models.Model):
    global_id = models.CharField("ИД подраздела",max_length=1024, blank=True, null=True)
    name = models.CharField("Наименование",max_length=1024, blank=True, null=True)
    section = models.ForeignKey('Sections', verbose_name="Раздел", null=True)

    class Meta:
        verbose_name = 'Подраздел'
        verbose_name_plural = 'Подразделы'

class Pages(models.Model):
    global_id = models.CharField("ИД страницы",max_length=1024, blank=True, null=True)
    name = models.CharField("Наименование",max_length=1024, blank=True, null=True)
    subsection = models.ForeignKey('Subsections', verbose_name="Подраздел", null=True)

    class Meta:
        verbose_name = 'Страница'
        verbose_name_plural = 'Страницы'

class DataSet(models.Model):
    spark_count = models.CharField(max_length=255, blank=True, null=True, default="0")

    def df_spark_count(self):

        def el_in_line(line, els):
            b = []
            for el in els:
                b.append(el in line)
            return not any(b)

        def filter_log(line):  
            return el_in_line(line, ['/container/','Drupal','/instructor','{"username": ""','/info','edx.ui.lms.link_clicked','/jump_to','/progress','seek_video','play_video','pause_video','load_video','/xblock/','/xmodule/','edx.ui.lms.sequence.next_selected','stop_video','seq_goto','seq_next','problem_graded','speed_change_video','problem_check','/course/','edx.ui.lms.sequence.previous_selected','/masquerade','studio.lektorium.tv'])

        conf = SparkConf().setAppName('TestProjApp1').set('spark.pyspark.python', '/home/alex/big_data_edx/venvs/bin/python3').set('spark.pyspark.driver.python','/home/alex/big_data_edx/venvs/bin/python3')
        sc = SparkContext.getOrCreate(conf=conf)
        sql_sc = SQLContext(sc)
        logRDD = sc.textFile("uploads/uploads/*.gz")
        logRDD = logRDD.map(lambda line: line.split('{', 1)[1])
        char_elem = '{'
        logRDD = logRDD.map(lambda line: f'{char_elem}{line}')
        log = logRDD.filter(filter_log)
        df_log = sql_sc.read.json(log)
        df_log = df_log[['username','time','event_type','page']]
        new_column = F.when(df_log.event_type!='page_close', F.split('event_type','/')[5]).when(df_log.event_type=='page_close',F.split('page','/')[7]).otherwise('page_close')
        df_log_test = df_log.withColumn('event_type', new_column)
        df_log_test = df_log_test.filter(df_log_test.event_type != '')
        df_log_test1 = df_log_test.withColumn("id",F.monotonically_increasing_id())
        mydict = df_log_test1.toPandas().set_index('id').T.to_dict('list')
        self.spark_count = df_log_test1.count()
        self.save()
