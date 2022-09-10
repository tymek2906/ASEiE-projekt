import findspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import StructType, StructField, StringType
import requests
import gzip


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
        self._url2018_index = [r"crawl-data/CC-MAIN-2018-13/cc-index.paths.gz",
                               r"crawl-data/CC-MAIN-2018-17/cc-index.paths.gz"]
        self._url2019_index = [r"crawl-data/CC-MAIN-2019-13/cc-index.paths.gz",
                               r"crawl-data/CC-MAIN-2019-18/cc-index.paths.gz"]

    def get_indexes(self):
        return self._indexes

    def get_full_indexes(self):
        return self._full_indexes

    def get_filenames(self):
        return self._filenames

    def get_index_file(self, file_location):
        df = self._spark.read.text(file_location)



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

        #print(df.count(),"all")
        df = df.select(f.col('value').substr(1,2).alias('pl'),
                       f.regexp_extract(f.col('value'),"""filename.*charset""",0).alias('value')).\
            groupBy(f.col('value'), f.col('pl')).count()
        #print(df.count(), "group")
        df = df.filter(f.col('pl').rlike(r"""pl"""))
        #print(df.count(), "pl")
        # This could be optional as it only removes 20k rows of files that aren't warc files, and we could
        # just be using if to discern "if" the file should be downloaded
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
        file = requests.get(url)
        file_content_bytes = gzip.decompress(file.content)
        file_content_list = file_content_bytes.decode('utf-8').strip().split('\n')
        return file_content_list

    def get_cluster(self, list_of_urls):
        try:
            return list_of_urls[300]
        except IndexError:
            print("This isn't a list of urls containing a cluster")

    def process(self, url, idx_url):
        warc_file = self.get_full_url_s3(url)  # Get link to s3 warc file
        index_file_link = self.get_full_url_s3(idx_url)  # Get link to s3 index file
        index_file_downloaded = self.get_file(index_file_link)  # Download it to find the cluster file
        cluster_file = self.get_cluster(index_file_downloaded)  # Get link to s3 cluster file
        self.cluster_reader(cluster_file)  # Read cluster file and find indexes of files in index file containing pl
        self.index_paths_reader(index_file_link)  # Get full links to those index files
        for full_index in self._full_indexes:  # Iterate over the index files
            self.index_reader(full_index)  # Retrieve links to warc files and put them into self._filenames
        self._indexes = set()  # reset short indexes
        self._full_indexes = set()  # reset full indexes

    def find_warcs(self):
        self.process(self._url2018[0], self._url2018_index[0])
        self.process(self._url2018[1], self._url2018_index[1])
        self.process(self._url2019[0], self._url2019_index[0])
        self.process(self._url2019[1], self._url2019_index[1])


if __name__ == '__main__':

    findspark.init()
    spark = SparkSession.builder.config("spark.driver.memory", "15g").\
        config("spark.driver.maxResultSize", "4g").appName("SparkProject").getOrCreate()
    lr = LinkReader(spark)
    # 3. Get a list of short indexes that need to be checked
    index = lr.to_list(lr.cluster_reader('files/cluster.idx'))
    #
    # # Find the full paths from the short paths to the downloadable files
    # lr.index_paths_reader("files/april2019_index.paths")
    #
    # # This is one of the files given by the previous steps, read it this way and it will automatically add
    # # the filenames to the set inside the class
    # lr.index_reader(index)
    #
    # # After inputting every file retrieve the filenames
    # files = lr.get_full_indexes()