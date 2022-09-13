

# needed installation sc.install_pypi_package("pandas==0.25.1")
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
import pandas as pd
import findspark
import matplotlib.pyplot as plt

def readwarc(file, spark_session):

    warc_df = spark_session.read.text(file, lineSep='WARC-Type:')


    return warc_df


def process_warc(df):
    df = df.select(f.regexp_extract(f.col('value'), """WARC-Target-URI: [\s\S]*?[\n]""", 0).alias('uri'),
                   f.col('value'))

    df = df.select(f.col('value'),
                   f.regexp_extract(f.col('uri'), """[.]pl""", 0).alias('lang'))

    df = df.select(f.col('value')).where(f.col('lang') == '.pl')

    df = df.select(f.col('value').alias('text'))

    df = df.select(f.col('text'),f.regexp_extract(f.col('text'), """WARC-Date: [\s\S]*?T""", 0).alias('date'))

    df = df.select(f.regexp_replace(f.lower(f.col('text')), """[\W]+""", ' ').alias('text'),
                   f.col('date'))

    df = df.select(f.col('text'),f.regexp_replace(f.col('date'), "(WARC-Date: )|T", '').alias('date'))

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
    df1 = df.select(f.col('date').alias('date_1'), f.col('count').alias('count_1')).where(f.col('year') == year1)
    df2 = df.select(f.col('date').alias('date_2'), f.col('count').alias('count_2')).where(f.col('year') == year2)
    df = df1.join(df2,df1['date_1'] == df2['date_2'], how='outer')
    df = df.select(f.when( df.date_1.isNull(), df.date_2).otherwise(df.date_1).alias('date'),f.col('count_1'),f.col('count_2'))
    return df


def plot(df):
     pdf = df.toPandas()
     pdf.plot.bar(x="date")
     plt.show()


# pointlist = []
# for x in range(10,30):
#     pointlist.append((f"2017-03-{x}",x))
#     pointlist.append((f"2017-04-{x}", 2*x))
#     pointlist.append((f"2018-03-{x}", x+10))
#     pointlist.append((f"2018-04-{x}", 2*x+10))

findspark.init()
spark = SparkSession.builder.appName("SparkProject").getOrCreate()
print(spark)
    # dft = spark.createDataFrame(pointlist)
    # dft = dft.select(f.col('_1').alias('date'),f.col('_2').alias('count'))
warc_df = readwarc('CC-MAIN-20190425074144-20190425100144-00322.warc.wet.gz', spark)
df = process_warc(warc_df)
words = ["covid", "kot", "zielony"]
df = agregate(df, words)
df.show()
x = prepare_to_plot(df, 2018, 2019)
plot(x)
spark.stop()
