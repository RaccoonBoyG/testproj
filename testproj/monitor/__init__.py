from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName('TestProjApp')
sc = SparkContext.getOrCreate(conf=conf)