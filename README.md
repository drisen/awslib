# awslib Package

library for processing statistics files collections on AWS S3
- **PreProcess** class to define a tablename to be processed
- **listRangeObjects** Generator of S3 objects in bucket with initial prefix, prefix,
    additional subpath inclusively between rangeMin and rangeMax, and file name
    that matches the fileRE regular expression.
- **key_split** Split an object key into
    {'prefix': str, 'msec': int, 'tablename': str, 'version': int, 'suffix': str)}
- **print_selection** If verbose, print func(obj) for up to first 300 objects in selection.
