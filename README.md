Reindexer
========================

Reindexer is python convenience utility for reindexing elastic indices.  It provides error handling
and some sanity checking of counts as it process daily indices.  The reindexing process is done using
Logstash.  Every operation is performed using a template that can be tailored as needed.  Some replacement 
values are calculated but any number of other values can be supplied in the replacement.properties file.

Dependencies
------------------------
Relies on Logstash to perform reindexing.

Relies on curl to perform queries against elasticsearch.

Relies on a number of packages that should be standard install (I hope).

Everything is tested using Python version 3.4 and Python version 2.7

Usage
------------------------

reindexer.py -s 2016.01.06 -e 2016.01.06 -l INFO
 
State
-------------------------

Currently able to reindex a date range with sanity check of document totals before and after.

License
-------------------------------

See LICENSE file.  MPL 2.0.

Plan
-------------------------

There are a number of items I'd like to do (I'm a novice so some of these are just bad).

* get a better handle on properties
* add hooks for more pre and post processing
* correct path usage of logstash vs template
* allow for config file path to be specified
* use the types.json instead of what is hard coded
* reorganize to flow better
* write report to a file
* tests ;_;
* figure out how to make this installable
