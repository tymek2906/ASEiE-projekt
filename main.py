import time

from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import findspark
findspark.init()
spark = SparkSession \
    .builder \
    .appName("PythonWordCount") \
    .getOrCreate()


def readwarc(pos):
    df = spark.read.text(pos, lineSep='fetchTimeMs')
    df = df.select(f.regexp_extract(f.col('value'), """WARC-Target-URI: [\s\S]*?[\n]""", 0).alias('uri'),f.col('value'))
    df = df.select(f.col('uri'),f.col('value'),f.regexp_extract(f.col('uri'),"""[.]pl""",0).alias('lang'))
    df = df.select(f.col('uri'), f.col('value')).where(f.col('lang') =='.pl')
    df = df.select(f.col('uri'),f.regexp_extract(f.col('value'),"""WARC-Type: response[\s\S]*?</html>""",0).alias('text')).where(f.length(f.col('text'))>0)
    df = df.select(f.regexp_extract(f.col('text'),"""WARC-Target-URI: [\s\S]*?[\n]""",0).alias('uri'),f.regexp_extract(f.col('text'),"""<html [\s\S]*?</html>""",0).alias('text'),f.regexp_extract(f.col('text'),"""WARC-Date: [\s\S]*?T""",0).alias('date'))
    df = df.select(f.col('uri'),f.regexp_replace(f.lower(f.col('text')),"""[\W]+""",' ').alias('text'),f.col('date'))
    df = df.select(f.col('uri'),f.decode(f.col('text'),'ISO-8859-1').alias('text'),f.col('date'))
    df = df.select(f.regexp_replace(f.col('uri'),"WARC-Target-URI: ",'').alias('uri'),f.col('text'),f.regexp_replace(f.col('date'),"(WARC-Date: )|T",'').alias('date'))
    return df


def countwords(df,words):
    pomdf = (df.select(f.explode(f.split(f.col('text'), ' ')).alias('word'), f.col('date')).where(f.length('word') > 0))
    return pomdf.filter(pomdf.word.isin(words)).groupBy('word','date').count()


def agregate(df,words):
    pomdf = (df.select(f.explode(f.split(f.col('text'), ' ')).alias('word'), f.col('date')).where(f.length('word') > 0))
    return pomdf.filter(pomdf.word.isin(words)).groupBy('date').count()


def tolist(df):
    list0 = []
    for i in df.collect():
        list0.append(tuple(i))
    return list0

df = readwarc('CC-MAIN-20170322212946-00000-ip-10-233-31-227.ec2.internal.warc')
words = ["zupa","kot","zielony"]
y = countwords(df,words)
y.show()
list1 = tolist(y)
x = agregate(df,words)
x.show()
list2 = tolist(x)
spark.stop()
