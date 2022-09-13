from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField, StringType

class LinkReader:
    """
    Workflow:
    1. Read the index.paths file
    2. Download the cluster.idx file
    3. Put the cluster.idx file into the cluster_reader
    4. Run index_paths_reader to get the full paths
    5. Download the .*/cdx-00XXX.gz files that were returned from the cluster_reader
    6. Put every file into the index_reader, it returns a df of paths to check and also saves them to an internal
       _filenames variable, so after entering every downloaded */cdx-00XXX.gz file gotten from reading the cluster file
       you'll have a set of filenames that have Polish sites in them
    """

    def __init__(self, spark_instance: SparkSession):
        self._spark = spark_instance
        self._indexes = set()
        self._full_indexes = set()
        self._filenames = set()
        self._filenames_df = self._spark.createDataFrame([], StructType([StructField('filenames', StringType(), True)]))
        self._url_s3 = r"s3://commoncrawl/"
        self._url_http = r"https://data.commoncrawl.org/"
        self._url2018 = [r"crawl-data/CC-MAIN-2018-13/warc.paths.gz",
                         r"crawl-data/CC-MAIN-2018-17/warc.paths.gz"]
        self._url2019 = [r"crawl-data/CC-MAIN-2019-13/warc.paths.gz",
                         r"crawl-data/CC-MAIN-2019-18/warc.paths.gz"]
        self._url2018_index = [r"s3://commoncrawl/cc-index/collections/CC-MAIN-2018-13/indexes/",
                               r"s3://commoncrawl/cc-index/collections/CC-MAIN-2018-17/indexes/"]
        self._url2019_index = [r"s3://commoncrawl/cc-index/collections/CC-MAIN-2019-13/indexes/",
                               r"s3://commoncrawl/cc-index/collections/CC-MAIN-2019-18/indexes/"]
        self._urlcluster = [r"s3://commoncrawl/cc-index/collections/CC-MAIN-2018-13/indexes/cluster.idx",
                            r"s3://commoncrawl/cc-index/collections/CC-MAIN-2018-17/indexes/cluster.idx",
                            r"s3://commoncrawl/cc-index/collections/CC-MAIN-2019-13/indexes/cluster.idx",
                            r"s3://commoncrawl/cc-index/collections/CC-MAIN-2019-18/indexes/cluster.idx"]

    def get_indexes(self):
        return self._indexes

    def get_full_indexes(self):
        return self._full_indexes

    def get_filenames(self):
        return self._filenames

    def cluster_reader(self, file_location):
        df = self._spark.read.csv(file_location)

        df = df.where(f.col('_c0') == 'pl')
        df = df.withColumn('name', f.regexp_extract(f.col('_c1'), r"""(.*)\s(cdx\-[0-9]+\.gz)(.*)""", 2))

        df = df.withColumn(
            "name",
            f.when(
                f.col("name") == '',
                f.regexp_extract(f.col('_c2'), r"""(.*)\s(cdx\-[0-9]+\.gz)(.*)""", 2)
            ).
                otherwise(f.col("name")))

        df = df.withColumn(
            "name",
            f.when(
                f.col("name") == '',
                f.regexp_extract(f.col('_c3'), r"""(.*)\s(cdx\-[0-9]+\.gz)(.*)""", 2)
            ).
                otherwise(f.col("name")))

        df = df.dropDuplicates(['name'])
        df = df.filter((df.name != '') & (df.name != 'none'))
        df = df.select(f.col('name'))
        temp = self.to_list(df)
        self._indexes.update(temp)
        return df

    def to_list(self, df):
        return [row[0] for row in df.select('*').collect()]

    def index_reader(self, file_location, as_df=False):
        df = self._spark.read.text(file_location)

        df = df.select(f.col('value').substr(1, 2).alias('pl'),
                       f.regexp_extract(f.col('value'), """filename.*charset""", 0).alias('value')). \
            groupBy(f.col('value'), f.col('pl')).count()

        df = df.filter(f.col('pl').rlike(r"""pl"""))

        df = df.filter(f.col('value').contains('/warc/'))
        df = df.select(f.regexp_replace(f.col('value'), """(filename....)|(....charset)""", '').alias('value'))
        if not as_df:
            list_df = self.to_list(df)
            print(len(list_df))
            _urls = [self.get_full_url_s3(_url) for _url in list_df]
            self._filenames.update(_urls)
        else:
            self._filenames_df = self._filenames_df.union(df).dropDuplicates()
            print(self._filenames_df.count())
        return df

    def change_to_wet(self, file_location, as_df=False):
        df = self._spark.read.text(file_location)

        df = df.select(f.col('value').substr(1, 2).alias('pl'),
                       f.regexp_extract(f.col('value'), """filename.*charset""", 0).alias('value')). \
            groupBy(f.col('value'), f.col('pl')).count()

        df = df.filter(f.col('pl').rlike(r"""pl"""))

        df = df.filter(f.col('value').contains('/warc/'))
        df = df.select(f.regexp_replace(f.col('value'), """(filename....)|(....charset)""", '').alias('value'))
        df = df.select(f.regexp_replace(f.col('value'), r"/warc/", r"/wet/").alias('value'))
        df = df.select(f.regexp_replace(f.col('value'), r"warc.gz", r"warc.wet.gz").alias('value'))
        if not as_df:
            list_df = self.to_list(df)
            print(len(list_df))
            _urls = [self.get_full_url_s3(_url) for _url in list_df]
            self._filenames.update(_urls)
        else:
            self._filenames_df = self._filenames_df.union(df).dropDuplicates()
            print(self._filenames_df.count())
        return df

    def index_paths_reader(self, file_location):
        with open(file_location, mode='r') as file:
            _files = file.readlines()

        for file in _files[:-2]:
            short_idx = file.strip()[-12:]
            if short_idx in self._indexes:
                _url = self.get_full_url_s3(file)
                self._full_indexes.add(_url)
        self._indexes = set()

    def get_full_url_http(self, url: str):
        return self._url_http + url

    def get_full_url_s3(self, url: str):
        return self._url_s3 + url

    def get_file(self, url):
        # file_content_bytes = gzip.decompress(url)
        file_content_list = url.strip().split('\n')
        return file_content_list

    def get_cluster(self, list_of_urls):
        try:
            return list_of_urls[300]
        except IndexError:
            print("This isn't a list of urls containing a cluster")

    def get_full_url(self, l, url):
        i = []
        for x in l:
            i.append(url + x)
        return i

    def process(self, idx_url, cluster_id):
        # index_file_link = self.get_full_url_s3(idx_url)  # Get link to s3 index file
        # index_file_downloaded = self.get_file(index_file_link)  # Download it to find the cluster file
        # cluster_file = self.get_cluster(index_file_downloaded)  # Get link to s3 cluster file
        self.cluster_reader(cluster_id)  # Read cluster file and find indexes of files in index file containing pl
        index_file_list = self.get_full_url(self._indexes, idx_url)
        # self.index_paths_reader(index_file_list)  # Get full links to those index files
        self._indexes = set()  # reset short indexes
        self._full_indexes = set()  # reset full indexes
        return index_file_list

    def find_warcs(self):
        files = []
        files1 = self.process(self._url2018_index[0], self._urlcluster[0])
        files2 = self.process(self._url2018_index[1], self._urlcluster[1])
        files3 = self.process(self._url2019_index[0], self._urlcluster[2])
        files4 = self.process(self._url2019_index[1], self._urlcluster[3])
        for x in files1:
            files.append(x)
        for x in files2:
            files.append(x)
        for x in files3:
            files.append(x)
        for x in files4:
            files.append(x)
        self.index_reader(files)  # Retrieve links to warc files and put them into self._filenames

    def find_wets(self):
        files = []
        files1 = self.process(self._url2018_index[0], self._urlcluster[0])
        files2 = self.process(self._url2018_index[1], self._urlcluster[1])
        files3 = self.process(self._url2019_index[0], self._urlcluster[2])
        files4 = self.process(self._url2019_index[1], self._urlcluster[3])
        for x in files1:
            files.append(x)
        for x in files2:
            files.append(x)
        for x in files3:
            files.append(x)
        for x in files4:
            files.append(x)
        self.change_to_wet(files)  # Retrieve links to warc files and put them into self._filenames

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

spark = SparkSession.builder.appName("SparkProject").getOrCreate()
lr = LinkReader(spark)
lr.find_wets()
files = lr.get_filenames()
warc_df = readwarc(files, spark)
df = process_warc(warc_df)
words = ["covid", "kot", "zielony"]
df = agregate(df, words)
df.show()
x = prepare_to_plot(df, 2018, 2019)
x.coalesce(1).write.csv(r"s3://sparkprojektbucket/output")
spark.stop()

