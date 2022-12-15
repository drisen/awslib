#
# awsCache.py Copyright (C) 2021 Dennis Risen, Case Western Reserve University
#
""" To Do
Enforce maximum space usage by cache files that are over N days old
- on reader.close(), update the modified date
    e.g. by opening for append, then closing w/o actually writing
- allow for concurrent multiple copies of the class
- update estimated cache size on each writer.close() and file deletion
- [re]initialize estimated cache size on class creation and

- check size on cache creation, and percentage of  writer.close()
cache creation, explicit expunge,
- calculate
See collect/s3Cache
"""

import os
import os.path as os_path
import time
from botocore.config import Config
import boto3


cachePath: str = os.getenv('cachePath', os.path.join(os.environ['TMP'], 'AWSCache'))
# mapping from a file read mode to the corresponding write mode
mode_map = {'r': 'w', 'rt': 'wt', 'rb': 'wb'}
# suffix applied to prospective cache file while being written
tmp_suffix = '.cache_tmp'


class AWSCache:
    """Access AWS S3 objects through a cache maintained in ~/cache

    """
    config = Config(retries={'max_attempts': 5, 'mode': 'standard'})
    s3 = boto3.resource('s3', config=config)
    # ensure that the cache directory exists
    os.makedirs(cachePath, exist_ok=True)

    @classmethod
    def expire(cls, age: float = -1.0, m_bytes: float = -1.0) -> bool:
        """Remove expired or incomplete contents and/or trim cache to <= m_bytes

        :param age:         maximum days since last access. <0 --> don't expire
        :param m_bytes:     maximum cache size in MB. <0 --> don't limit size
        :return:
        """
        # folder directory with last access time, size, and filename
        max_bytes = m_bytes if m_bytes > 0 else 10.0**10
        try:
            entries = [(de.stat().st_atime, de.stat().st_size, de.name)
                   for de in os.scandir(cachePath) if de.is_file()]
        except FileNotFoundError:
            os.makedirs(cachePath, exist_ok=True)
            entries = [(de.stat().st_atime, de.stat().st_size, de.name)
                   for de in os.scandir(cachePath) if de.is_file()]
        entries.sort(reverse=True)          # descending by time of last access
        horizon = time.time() - 24*60*60*age
        tot_size = 0.0
        for t, size, fn in entries:
            tot_size += size/1000000.0  # include in total size
            if t < horizon or tot_size >= max_bytes or fn.endswith(tmp_suffix):
                try:
                    os.remove(os_path.join(cachePath, fn))
                    tot_size -= size/1000000.0  # except when successfully removed
                except OSError:
                    print(f"Couldn't delete {os_path.join(cachePath, fn)}")
        return tot_size <= max_bytes

    @classmethod
    def open(cls, bucket: str, key: str, read_all: bool = False, **kwargs):
        """Open AWS object for reading from filesystem cache or AWS S3

        :param bucket:  The AWS bucket
        :param key:     The AWS object key
        :param read_all: read the object and return bytes or str
        :kwargs:        additional keyword arguments for {io/gzip}.open

        """
        filename = os_path.join(cachePath, key.rpartition('/')[-1])
        if filename[-3:] == '.gz':  # is object gzip compressed binary?
            filename = filename[:-3]  # Yes. adjust to uncompressed name
            if os_path.isfile(filename):  # is file in the cache?
                # Yes. Simply open the cached file
                return open(filename, **kwargs)
        # Object is not in the cache. Create AWSCache instance to read the object
        if read_all:
            return AWSCache(bucket, key, filename, **kwargs).read()
        else:
            return AWSCache(bucket, key, filename, **kwargs)

    def __init__(self, bucket: str, key: str, filename: str, **kwargs):
        """Open AWS object for reading from AWS and writing to cache

        :param bucket:  The AWS bucket
        :param key:     The AWS object key
        :**kwargs:      additional arguments for io.open or gzip.open

        """
        self.filename = filename
        self.closed = False
        aws_stream = AWSCache.s3.Object(bucket_name=bucket, key=key).get()['Body']
        self.readers = [aws_stream]
        if key[-3:] == '.gz':      # is object gzipped?
            shim = gzip.open(aws_stream, **kwargs)
            self.in_stream = shim
            self.readers.insert(0, shim)
        else:                           # AWS file is not compressed
            self.in_stream = aws_stream
        # map from specified (or default) read mode to corresponding write mode
        mode = mode_map[kwargs.get('mode', 'rt')]
        if not os.path.isdir(cachePath):  # no cache directory?
            os.makedirs(cachePath, exist_ok=True)  # create it
        self.writer = open(filename + tmp_suffix, mode)  # fp to write to the cache

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        # False to propagate an exception
        return None if exc_type is None and exc_val is None and exc_tb is None else False

    def close(self):
        """Copy any unread tail of the object to tmp; Close and rename tmp to cache

        """
        while not self.writer.closed:
            if len(self.read()) == 0:   # copy remaining to cache, and close the writer
                self.writer.close()
                os.rename(self.filename + tmp_suffix, self.filename)
        for fp in self.readers:
            if not getattr(fp, 'closed', True):
                fp.close()
        self.closed = True

    def read(self, size=None):
        """Read from uncompressed object stream while copying through to cache

        """
        result = self.in_stream.read(size)
        if len(result) > 0:            # more bytes?
            self.writer.write(result)
        return result

    @staticmethod
    def readable(self):
        """AWSCache instance is always readable

        :param self:
        :return:
        """
        return True

    @staticmethod
    def seekable(self):
        """AWSCache instance is not seekable
        :param self:
        :return:
        """
        return False

    def __del__(self):
        """Ensure that cache file is completely written and closed

        """
        if not self.closed:
            self.close()

    # def seek(self, offset, whence=SEEK_SET):  # not implemented
    # def tell(self):  # not implemented

    @staticmethod
    def writable(self):
        """
        AWSCache instance is not readable
        :param self:
        :return:
        """
        return False

    # def readall(self):
    # def readint(self, buf):  # not implemented
    """Performance notes using 3 HistoricalClientCount.csv files covering 9 hours.
    The uncompressed files totalled 259MB.
    Read performance from the cache should allow reading 2 such streams at a
    fast-forward rate of 1 simulated hour per 2 seconds. E.g. 1 day in 48 seconds.
    The actual performance reading from disk through csv.DictReader was
    9 simulated hours in 16.6 to 19.8 seconds, dominated by per-record csv
    processing. I.e. 1 simulated hour in 2.7--3.3 seconds.
    
    Performance could be enhanced by redefining AWSCache.read to deliver a dict per
    object's csv record, rather than a text line to be processed by csv.DictReader.
    AWSCache.reader would include csv.DictReader to deliver a dict for each record.
    AWSCache.writer would aggregate all records in each file into a Python list,
    then on file close, in a separate thread, pickle.dump the list to the cache.
    Access CPU during the initial read+cache_write would be similar, but would incur
    an about 1.9 second CPU chug on each file close.
    On access from the cache, pickle.load would incur an about 2.6
    second CPU chug before delivering the first record in a file, but then deliver
    each record in microseconds. During read from the cache, CPU for the same 3
    files of 9 hours is about 7.75 seconds. I.e. 1 simulated hour in 0.9 seconds.
    Partitioning each csv file (~1.4M lines in the above test) into smaller chunks
    of <=100K lines adds complexity, but would reduce the CPU chugs to an acceptable
    <200msec while not significantly increasing total CPU.
    """


if __name__ == '__main__':
    import gzip
    import csv
    cases = ("Stream from AWS through gzip to disk",
             "Buffered read from disk",
             "Buffered read from disk w DictReader")
    bucket = 'cwru-data'
    prefix = 'network/wifi/ncsdata/dar5'
    APD_eg = (('2021/03/30', '1617107308322_AccessPointDetailsv4.csv.gz'),
              ('2021/04/02', '1617395308614_AccessPointDetailsv4.csv.gz'),
              ('2021/10/11', '1633960276918_AccessPointDetailsv4.csv.gz'))
    HCC_eg = (('2021/04/10', '1618105577372_HistoricalClientCountsv4.csv.gz'),
              ('2021/04/12', '1618256777768_HistoricalClientCountsv4.csv.gz'),
              ('2021/04/19', '1618839979040_HistoricalClientCountsv4.csv.gz'))

    AWSCache.expire(0.0, 0.0)           # clear the cache
    fieldnames = ('@id', 'dot11aAuthCount', 'dot11n5Count')
    for tn, example in (('AccessPointDetails', APD_eg),
                        ('HistoricalClientCounts', HCC_eg)):
        # get timing to read tables from AWS vs from cache
        for i in range(3):
            startTime = time.time()
            # get timing to load some tables
            for yyyy_mm_dd, fn in example:
                key = '/'.join([prefix, tn, yyyy_mm_dd, fn])
                print(f"opening {bucket}/{key}")
                with AWSCache.open(bucket, key, mode='rt', newline='') as reader:
                    if i < 2:
                        reader.read()
                    else:
                        csv_reader = csv.DictReader(reader)  # 16.60 seconds
                        # csv_reader = csv.DictReader(reader, fieldnames=fieldnames)  # 19.84 seconds
                        tbl = [rec for rec in csv_reader]
            print(f"{time.time()-startTime:0.2f} seconds for 3 tables: {cases[i]}")
    # AWSCache.expire(1, 100)
