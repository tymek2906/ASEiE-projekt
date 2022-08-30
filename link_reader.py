import findspark
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
        self._url = r"https://data.commoncrawl.org/"

    def get_indexes(self):
        return self._indexes

    def get_full_indexes(self):
        return self._full_indexes

    def get_filenames(self):
        return self._filenames

    def append_url(self, text):
        return self._url+text

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
        print(df.count())
        df = df.select(f.regexp_extract(f.col('value'), r"""(^pl.*)""", 1).alias('pl')).filter(f.col('pl') != '')
        print(df.count())

        # This could be optional as it only removes 20k rows of files that aren't warc files, and we could
        # just be using if to discern "if" the file should be downloaded
        df = df.select(f.regexp_extract(f.col('pl'), r""".*filename\":\s\"(crawl.*warc.*CC\-MAIN.*gz)\".*""", 1).
                       alias("filenames")).filter(f.col('filenames') != '')


        if not as_df:
            list_df = self.to_list(df)
            print(len(list_df))
            _urls = [self.append_url(_url) for _url in list_df]
            self._filenames.update(_urls)
        else:
            self._filenames_df = self._filenames_df.union(df).dropDuplicates()
            print(self._filenames_df.count())
        return df

    def index_paths_reader(self, file_location):
        with open(file_location, 'r') as file:
            _files = file.readlines()

        for file in _files[:-2]:
            short_idx = file.strip()[-12:]
            if short_idx in self._indexes:
                _url = self.append_url(file)
                self._full_indexes.add(_url)


if __name__ == '__main__':
    findspark.init()
    spark = SparkSession.builder.appName("SparkProject").getOrCreate()
    lr = LinkReader(spark)

    # 3. Get a list of short indexes that need to be checked
    index = lr.to_list(lr.cluster_reader('files/april2019_cluster.idx'))

    # Find the full paths from the short paths to the downloadable files
    lr.index_paths_reader('files/april2019_index.paths')

    # This is one of the files given by the previous steps, read it this way and it will automatically add
    # the filenames to the set inside the class
    lr.index_reader('files/cdx_april2019', True)

    # After inputting every file retrieve the filenames
    files = lr.get_full_indexes()
