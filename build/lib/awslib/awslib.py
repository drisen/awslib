import boto3
import re
from cpi.cpitable import SubTable, find_table
from cpi.cpitables import production

casters = {'cast:bigint': lambda x: int(x),
		'cast:boolean': lambda x: x[0].upper() == 'T' if len(x) > 0 else None,
		'cast:double': lambda x: float(x),
		'cast:float': lambda x: float(x),
		'cast:int': lambda x: int(x),
		'cast:long': lambda x: int(x)}
re_cast = {'boolean': 'cast:boolean', 	# dictionary of types that need recasting
		'double': 'cast:double',
		'epochMillis': 'cast:long',
		'float': 'cast:float',
		'int': 'cast:int',
		'long': 'cast:long',
		}


class PreProcess:
	def __init__(self, table_name: str, version: int = None):
		"""

		Args:
			table_name:		CPI table name
			version: 		Version number
		"""
		self._drop_fields_: list = [] 	# [field_name: str, ...]
		self.key_source: str = None
		self.id_func: callable = lambda x: x
		self.table_name = table_name
		self._resolve_choice_: dict = {}  # {field_name: spec, ...}
		self._mapper_: callable = lambda x: x  # function(record) for field mapping
		self.fp = None

		# Ensure that non-string fields in CSV file are properly recast
		# By adding resolve_choice for each field which needs to ve cast, but does not have a resolve choise entry
		tbl: SubTable = find_table(self.table_name, [production], version)
		for field_name in tbl.select: 	# for each SELECTed field
			field_type = tbl.fieldTypes[field_name]  # 'boolean', Date, ...
			casting = re_cast.get(field_type['name'], None)
			if casting is not None and field_name not in self._resolve_choice_:	 # This field type needs to be cast
				self._resolve_choice_[field_name] = casting

	def __iter__(self):
		max_errs = 1
		for record in self.fp:
			err_lst = []
			self._mapper_(record)  # map fields to canonic form
			for field_name, spec in self._resolve_choice_.items():  # re-cast fields
				try:
					func = casters[spec]
				except KeyError:
					raise KeyError(f"Undefined resolveChoice spec={spec}")
				except TypeError:
					print(f"Bad resolve_choice spec {spec} for {field_name}")
					raise ValueError
				try:
					record[field_name] = func(record[field_name])
				except ValueError:
					if len(record[field_name]) == 0:  # empty field?
						record[field_name] = None  # Yes. Represent as None
					else:
						err_lst.append(f"can't recast {field_name} as {spec}")
				except KeyError:
					err_lst.append(f"{field_name} is missing")
			if len(err_lst) > 0:
				if max_errs > 0:
					print(f"resolve_choice {', '.join(err_lst)}")
				elif max_errs == 0:
					print(f"resolve_choice ...")
				max_errs -= 1
			for x in self._drop_fields_:  # Drop each field in _drop_fields_
				try:
					del record[x]
				except KeyError:
					pass
			yield record

	def __str__(self):
		s = f"(resolve_choice={[(k,v) for (k,v) in self._resolve_choice_.values()]}, drop_fields={self._drop_fields_}"
		return s

	def drop_field(self, fields: list):
		self._drop_fields_.extend(fields)

	def key(self, key_source: str):
		self.key_source = key_source
		self.id_func = eval(key_source)	 # typically the stream is ordered by the same key

	def id(self, id_func: callable):
		self.id_func = id_func

	def mapper(self, mapper: callable):
		self._mapper_ = mapper

	def resolve_choice(self, field_tuples: list):
		"""Each tuple in list is (field_name, 'cast:type:)"""
		for tup in field_tuples:
			self._resolve_choice_[tup[0]] = tup[1]

	def set_reader(self, fp):
		self.fp = fp
		return self


def listRangeObjects(prefix: str, range_min: str, range_max: str,
					range_index: int, file_re: str, verbose: int = 0) -> dict:
	"""Generator of S3 objects in bucket with initial prefix, prefix,
	additional subpath inclusively between rangeMin and rangeMax, and file name
	that matches the fileRE regular expression. Each yielded object is
	{'Bucket':str, 'Key':str, 'LastModified':datetime(), 'ETag':str, 'Size':int, 'StorageClass':str, 'Owner':dict()}

	Parameters:
		prefix (str:)		bucket +initial prefix '' (w/ or w/o trailing separator)
		range_min (str:)		minimum subpath or '' (w/o trailing separator)
		range_max (str:)		maximum subpath or '' (w/o trailing separator)
		range_index (int:)	starting index of range in the components of Key
		file_re (str:)		regular expression filter for final file name
		verbose (int):		diagnostic message level
	Generator that yields:
		dict of S3 object as returned from s3 client list_objects_v2

	"""
	bucket, s, prefix = prefix.partition('/')
	range_cnt = range_max.count('/') + (0 if len(range_max) == 0 else 1)
	filter_prefix = prefix.split('/')
	final_sep = (1 if filter_prefix[-1] == '' else 0)  # 1 iff trailing '/'
	if range_index == len(filter_prefix) - final_sep:  # range immediately follows the prefix?
		# Yes, extend prefix w/ greatest common subpath of the range
		if final_sep == 1:			# a trailing separator?
			filter_prefix.pop() 	# remove it for now
		if verbose > 0:
			print("range immediately follows the prefix")
		min_list = range_min.split('/') 	# split each subpath into a list of accessors
		max_list = range_max.split('/')
		i = 0
		while i < len(min_list) and i < len(max_list) and min_list[i] == max_list[i]:
			i += 1					# number of components in greatest common prefix

		min_prefix = filter_prefix.copy()
		max_prefix = filter_prefix.copy()
		if i > 0:				# a non-trivial gcp?
			filter_prefix.extend(min_list[:i])  # extend with greatest common prefix
		else:
			filter_prefix.append('')  # terminate with a separator"
		if len(range_min) > 0:
			min_prefix.append(range_min)
		if len(range_max) > 0:
			max_prefix.append(range_max)
		filter_prefix = '/'.join(filter_prefix)   # Join each list with '/' key delimiter
		min_prefix = '/'.join(min_prefix)
		max_prefix = '/'.join(max_prefix)
		if verbose > 0:
			print(f"filter_prefix={filter_prefix},\nmin_prefix   ={min_prefix},\nmax_prefix   ={max_prefix}.")
	else:							# No, filter_prefix is merely the prefix
		filter_prefix = prefix
	range_pat = ('[^/]+/'*range_cnt)
	if len(range_pat) > 0:
		range_pat = "(" + range_pat[:-1] + ")/"  # move final '/' outside the group
	pat = f"{'[^/]+/' * range_index}" + range_pat + "([^/]+/)*([0-9]+_[a-zA-Z0-9_]+" + r"\..*)"
	if verbose > 0:
		print(f"pat={pat}")
	pat = re.compile(pat)
	client = boto3.client('s3')
	paginator = client.get_paginator('list_objects_v2')
	operation_parameters = {'Bucket': bucket, 'Prefix': filter_prefix, 'FetchOwner': True}
	pageIterator = paginator.paginate(**operation_parameters)
	for page in pageIterator:		# for each each page of objects
		if verbose > 1:
			print(f"type(page)={type(page)}. value={str(page)[:130]}")
		try:
			contents = page['Contents']
			# print(f"contents={str(contents)[:130]}")
			for obj in contents: 	# for each object in the page
				# print(f"type(obj)={type(obj)}. value={str(obj)[:130]}")
				obj['Bucket'] = bucket
				# print(f"Key={obj['Key']}")
				m = re.match(pat, obj['Key'])
				if m is None:
					print(f"{obj['Key']} doesn't match expected form. Ignored.")
					continue
				range_fields = m.group(1)
				fileName = m.group(3)
				# print(range_fields, fileName)
				if range_min <= range_fields <= range_max and re.search(file_re, fileName) is not None:
					yield obj		# yield obj
		except Exception as e:
			print(f"{e} in listRangeObjects")
			return

splitpat = r'(.*)/([0-9]+)_([^\.]*)\.(.*)'
splitsubtable = r'([^_]+)v([0-9]+)(_.+)'
splittable = r'(.+)v([0-9]+)'


def key_split(key: str) -> dict:
	"""Split an object key into
	{'prefix': str, 'msec': int, 'tablename': str, 'version': int, 'suffix': str)}"""
	result = {}
	m = re.fullmatch(splitpat, key)
	try:
		result['prefix'] = m.group(1)
		result['msec'] = int(m.group(2))
		tablename = m.group(3)
		result['suffix'] = m.group(4)
	except (AttributeError, IndexError):  # object name does not match pattern
		return None
	# extract the version number from the tablename string
	m = re.fullmatch(splitsubtable, tablename)
	if m:							# form: basetable vN _ subtable
		result['version'] = int(m.group(2))
		result['tablename'] = m.group(1) + m.group(3)
	else:
		m = re.fullmatch(splittable, tablename)
		if m:						# form: table vN
			result['version'] = int(m.group(2))
			result['tablename'] = m.group(1)
		else:						# no version number
			result['version'] = 0 	# version 0 indicates unspecified
			result['tablename'] = tablename
	return result


def print_selection(selection: list, func: callable, verbose: int = 0):
	"""If verbose, print func(obj) for up to first 300 obj in selection

	Args:
		selection: 	list of objects
		func: 		function to format each object
		verbose: 	True to print; False to do nothing

	"""
	limit = 300						# Maximum number of objects to be listed
	if verbose > 0:
		if len(selection) > limit:
			print(f"Only the first {limit} objects will be listed")
		for obj in selection:
			if limit <= 0:
				print('...')
				break
			limit -= 1
			print(func(obj))
	print(f"{len(selection)} objects are selected")
