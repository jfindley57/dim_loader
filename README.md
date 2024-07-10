# Sample Code for a dimension program
### This code would dump dimension data from a mysql DB, move to a s3 location, and load into a Vertica database


Code flow goes like: \
dimension_wrapper.py >>> dimensions_to_s3.py >>> vertica_load.py 

All other files are supporting the ones listed above