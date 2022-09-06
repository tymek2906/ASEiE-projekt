import time

from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import findspark
import matplotlib.pyplot as plt
import pandas as pd

def readwarc(file, spark_session):

    warc_df = spark_session.read.text(file, lineSep='fetchTimeMs')


    return warc_df


def process_warc(df):
    df = df.select(f.regexp_extract(f.col('value'), """WARC-Target-URI: [\s\S]*?[\n]""", 0).alias('uri'),
                   f.col('value'))

    df = df.select(f.col('uri'),
                   f.col('value'),
                   f.regexp_extract(f.col('uri'), """[.]pl""", 0).alias('lang'))

    df = df.select(f.col('uri'),
                   f.col('value')).where(f.col('lang') == '.pl')

    df = df.select(f.col('uri'),
                   f.regexp_extract(f.col('value'), """WARC-Type: response[\s\S]*?</html>""", 0).alias('text')) \
        .where(f.length(f.col('text')) > 0)

    df = df.select(f.regexp_extract(f.col('text'), """WARC-Target-URI: [\s\S]*?[\n]""", 0).alias('uri'),
                   f.regexp_extract(f.col('text'), """<html [\s\S]*?</html>""", 0).alias('text'),
                   f.regexp_extract(f.col('text'), """WARC-Date: [\s\S]*?T""", 0).alias('date'))

    df = df.select(f.col('uri'),
                   f.regexp_replace(f.lower(f.col('text')), """[\W]+""", ' ').alias('text'),
                   f.col('date'))

    df = df.select(f.col('uri'),
                   f.decode(f.col('text'), 'ISO-8859-1').alias('text'),
                   f.col('date'))

    df = df.select(f.regexp_replace(f.col('uri'), "WARC-Target-URI: ", '').alias('uri'),
                   f.col('text'),
                   f.regexp_replace(f.col('date'), "(WARC-Date: )|T", '').alias('date'))

    return df


def countwords(df, words):
    pomdf = df.select(f.explode(f.split(f.col('text'), ' ')).alias('word'), f.col('date')).where(f.length('word') > 0)
    return pomdf.filter(pomdf.word.isin(words)).groupBy('word', 'date').count()


def agregate(df, words):
    pomdf = df.select(f.explode(f.split(f.col('text'), ' ')).alias('word'), f.to_date(f.col("date"),"yyyy-mm-dd").alias('date')).where(f.length('word') > 0)
    return pomdf.filter(pomdf.word.isin(words)).groupBy('date').count()


def tolist(df):
    list0 = []
    for i in df.collect():
        list0.append(tuple(i))
    return list0


def prepare_to_plot(df,year1,year2):
    df = df.sort(f.col('date'))
    df = df.select(f.col('date').substr(0,4).alias('year'),f.col('date').substr(6,5).alias('date'),f.col('count'))
    df1 = df.select(f.col('date').alias('date 1'), f.col('count').alias('count 1')).where(f.col('year') == year1)
    df2 = df.select(f.col('date').alias('date 2'), f.col('count').alias('count 2')).where(f.col('year') == year2)
    df = df1.join(df2,df1['date 1'] == df2['date 2'])
    df = df.select(f.col('date 1').alias('date'),f.col('count 1'),f.col('count 2'))
    return df


def plot(df):
    pdf = x.toPandas()
    pdf.plot.bar(x="date")
    plt.show()


# pointlist = []
# for x in range(10,30):
#     pointlist.append((f"2017-03-{x}",x))
#     pointlist.append((f"2017-04-{x}", 2*x))
#     pointlist.append((f"2018-03-{x}", x+10))
#     pointlist.append((f"2018-04-{x}", 2*x+10))


if __name__ == '__main__':
    findspark.init()
    spark = SparkSession.builder.appName("SparkProject").getOrCreate()
    print(spark)
    # dft = spark.createDataFrame(pointlist)
    # dft = dft.select(f.col('_1').alias('date'),f.col('_2').alias('count'))
    warc_df = readwarc('files/test.warc', spark)
    df = process_warc(warc_df)
    words = ["zupa", "kot", "zielony"]
    #y = countwords(df, words)
    #y.show()
    #list1 = tolist(y)
    #print(list1)
    df = agregate(df, words)
    x = prepare_to_plot(df,2017,2018)
    plot(x)
    list2 = tolist(df)
    spark.stop()


