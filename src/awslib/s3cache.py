#
# Copyright 2020 Dennis Risen, Case Western Reserve University
#

from boto3 import resource
from io import BufferedIOBase
from os.path import join
from os import listdir, mkdir, remove, rename, scandir, stat
from threading import RLock
from time import time
""" To Do

"""


class S3cache:
    max_size = 10*1000*1000*1000 	# Maximum cache storage

    def __init__(self, path: str):
        """A cache of recently read S3 objects.

        :param path: 	path to file-system directory for cache
        """
        self._path = path 			# path to file-system root directory for cache
        self._cache = {}  # {bucket+path: {'path': str, 'st_size': int, 'st_atime': float, 'st_mtime': float}, ...}
        self._lock = RLock() 		# critical section lock for the class
        self._size = 0					# cache size in bytes
        self.s3 = resource('s3')		# S3 resource for all reads
        found_temp = False
        with scandir(path) as it:		# load cache from disk
            for entry in it:
                if entry.is_file(follow_symlinks=False):
                    s = entry.stat(follow_symlinks=False)
                    self._cache[entry.name.replace(';', '/')] = {
                        'path': join(path, entry.name), 'st_size': s.st_size,
                        'st_atime': s.st_atime, 'st_mtime': s.st_mtime}
                    self._size += s.st_size
                elif entry.name == 'temp' and entry.is_dir(follow_symlinks=False):
                    found_temp = True
                    temp_dir = join(path, 'temp')
                    for tmp in listdir(temp_dir): 	# remove all temp files in temp_dir
                        try:
                            remove(join(temp_dir, tmp))
                        except OSError:				# ignore errors
                            pass
        if not found_temp:				# no directory for temp files?
            mkdir(join(path, 'temp')) 	# create it

    def __delitem__(self, obj: str):
        with self._lock:				# to update total bytes and length of cache
            entry = self.__getitem__(obj)
            try:
                remove(entry['name']) 	# delete the cached file
            except OSError:				# file is gone or in use
                raise KeyError
            del self._cache[obj]
            self._size -= entry['st_size']

    def __getitem__(self, obj: str):
        return self._cache[obj]

    def get(self, obj: str, default):
        try:
            return self.__getitem__(obj)
        except KeyError:
            return default

    def __setitem__(self, obj: str, path: str):
        """Add file named path to the cache as a copy of S3 bucket/key = obj

        :param obj: 		S3 bucket/key
        :param path: 		pathname of closed temp file
        :return:
        """
        fn = join(self._path, obj.replace('/', ';'))
        rename(path, fn)
        s = stat(fn, follow_symlinks=False)
        with self._lock:
            self._cache[obj] = {'name': path, 'st_size': s.st_size,
                'st_atime': s.st_atime, 'st_mtime': s.st_mtime}
            self._size += s.st_size
            while self._size > S3cache.max_size:  # exceeds maximum cache size?
                min_key = ''			# Yes. Find the least recently used key
                min_atime = time() + 60*60  # a minute in the future
                for obj, entry in self._cache.items():
                    if entry['st_atime'] < min_atime:
                        min_atime = entry['st_atime']
                        min_key = obj
                if len(min_key) == 0: 	# couldn't find a[nother] file to delete
                    return
                try:
                    self.__delitem__(min_key) 	# delete file from cache
                except KeyError:		# couldn't delete the file at this time
                    self._cache[min_key]['st_atime'] = time() + 2*60  # 2 minutes in the future
                    continue			# continue looking for file[s] to delete

    def Reader(self, obj: str, mode: str = 'rb', write_cache: bool = True) -> BufferedIOBase:
        """Return a BufferedReader to read from S3 or cache

        :param obj: 				S3 bucket/key
        :param mode:				read mode. default='rb'
        :param write_cache:			write to cache, if not in cache. default=True
        :return:					a file-like reader
        """
        with self._lock:
            entry = self._cache.get(obj, None)
            if entry is not None: 	# a hit in the cache?
                return open(entry['name'], mode=mode)  # Yes. Return file reader
            i = obj.index('/') 		# separate bucket from key
            reader = self.s3.Object(bucket_name=obj[:i], key=obj[i+1:]).get()['Body']
            if not write_cache: 	# just reading w/o writing to cache?
                return reader		# Yes. Return an AWS object reader
            return S3Reader(cache=self, obj=obj, reader=reader)


class S3Reader(BufferedIOBase):
    def __init__(self, cache: S3cache, obj: str, reader: BufferedIOBase):
        """A buffered S3 reader which adds the retrieved object to a local cache

        :param cache: 				# The file cache
        :param obj:					# S3 bucket/key
        :param reader:				# AWS S3 object reader
        """
        self.cache = cache			# The file cache
        self.obj = obj
        self.reader = reader		# the AWS S3Reader
        self.path = join(cache._path, 'temp', obj.replace('/', ';'))
        self.writer = open(self.path, 'wb')

    def close(self) -> None:
        if self.closed:				# already closed?
            raise ValueError		# ***** diagnostic during testing
            # return					# convenience return
        # copy the remainder of the file into the cache file
        while True:
            b = self.reader.read1()
            if len(b) == 0:			# at EOF?
                break
            self.writer.write(b)
        self.reader.close()
        with self.cache._lock:
            self.writer.close()
            self.cache.__setitem__(obj=self.obj, path=self.path)
        super().close()				# presumably this sets self.closed to True

    def read(self, __size: int = -1) -> bytes:
        b = self.reader.read(__size)
        self.writer.write(b)
        return b

    def read1(self, __size: int = -1) -> bytes:
        b = self.reader.read1(__size)
        self.writer.write(b)
        return b

    def readinto(self, __buffer) -> int:
        i = self.reader.readinto(__buffer)
        self.writer.write(__buffer[:i])
        return i

    def readinto1(self, __buffer) -> int:
        i = self.reader.readinto1(__buffer)
        self.writer.write(__buffer[:i])
        return i

    def seekable(self) -> bool:
        return False

    def writable(self) -> bool:
        return False
