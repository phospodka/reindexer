#!/usr/bin/env python

import getopt
import logging
import logging.config
import json
import os
import shlex
import sys
import time

from datetime import datetime, timedelta
from subprocess import Popen, PIPE

# logger definition
logger = logging.getLogger(__name__)
default_logger_level = logging.INFO


class Reindexer:

    # logstash executable
    _logstash = 'logstash'

    # global strings
    _CORE = 'replacement.core.'
    _DEF = 'replacement.def.'

    _core_props = {}
    _props = {}
    _prefix = '${'
    _suffix = '}'

    # I made these configurable for some reason ...
    _date = 'date'
    _dest_index = 'dest_index'
    _source_index = 'source_index'
    _type = 'type'

    # {type : {source: prefix, dest: prefix}} for each index to process
    types = {"logs": {"source": "logstash-", "dest": "logstash-"}}

    # homedir of the python file to use for relative pathing
    homedir = os.path.dirname(os.path.realpath(sys.argv[0]))

    def __init__(self, logging_level):
        """
        Init the Reindexer
        :param logging_level: logging level to configure at
        """
        self.init_logging(logging_level)
        self._props = self.compute_properties()
        self._props['path'] = self.homedir + '/templates/'

    def expand_daterange(self, start, end):
        """
        Expands a start and end date into the list of all encompassing dates in that range.
        :param start: start date string in the yyyy.mm.dd format
        :param end: end date date string in the yyyy.mm.dd format
        :return: list of dates string in the yyyy.mm.dd format
        """
        dates = []

        start_date = datetime.strptime(start, '%Y.%m.%d')
        end_date = datetime.strptime(end, '%Y.%m.%d')

        while start_date <= end_date:
            dates.append(start_date.strftime('%Y.%m.%d'))
            start_date = start_date + timedelta(days=1)

        return dates

    def load_template(self, name):
        """
        Read in a template by name.
        :param name: template name to lookup, expecting to be sans .template
        :return: contents of template as a string
        """
        template = ''
        filename = self.homedir + '/templates/' + name + '.template'
        # maybe make some sort of trace logging for this? and a few other things?
        # logger.debug('loading template: ' + filename)

        with open(filename, 'r') as f:
            for line in f:
                template += line

        return template

    def replace_template(self, name):
        """
        Get and replace values in a template with the loaded properties.
        :param name: template name to lookup, expecting to be sans .template
        :return: template string with replaced values
        """
        template = self.load_template(name)

        for key, value in self._props.items():
            template = template.replace(self._prefix + key + self._suffix, value)

        return template

    def invoke_logstash_template(self, name):
        """
        Get a logstash template and invoke it for the provided date.
        Similar to invoke_template but needed slightly different handling.
        :param name: name of logstash template, expecting to be sans .template
        :return: output of the command
        """
        logstash_config = self.replace_template(name)
        logger.debug(logstash_config)
        command = self._props['logstash_home'] + self._logstash + ' -e "' + logstash_config + '"'

        # fyi only use PIPE when using Popen -> communicate, don't use it with call
        p = Popen(shlex.split(command), stdin=PIPE, stdout=PIPE, stderr=PIPE)
        output, err = p.communicate()
        logger.debug(output)

        # anything but a 0 return code should be some sort of system error
        if p.returncode != 0:
            logger.error(output + err)
            raise RuntimeError(self._props['source_index'] + ' - error reindexing')

        return output

    def invoke_template(self, name):
        """
        Get the template and invoke it for the provided date.
        :param name: template name to lookup, expecting to be sans .template
        :return: output of the command
        """
        command = self.replace_template(name)
        logger.debug(command)

        # fyi only use PIPE when using Popen -> communicate, don't use it with call
        p = Popen(shlex.split(command), stdin=PIPE, stdout=PIPE, stderr=PIPE)
        output, err = p.communicate()
        logger.debug(output)

        # anything but a 0 return code should be some sort of system error
        if p.returncode != 0:
            logger.error(err)

        # decode is there for Python 3 as it is getting a binary stream atm and cannot be processed
        # by json.loads
        return output.decode("utf-8")

    def process_command(self, name, message, command_type):
        """
        Process the requested command for a certain date with log message info provided.
        :param name: template name to lookup, expecting to be sans .template
        :param message: description to use when logging info and errors
        :param command_type: type of command to use to determine what to log
        :return: output of the command
        """
        output = 'empty'
        try:
            output = self.invoke_template(name)
            json_output = json.loads(output)
            if command_type == 'count':
                logger.info(self._props['source_index'] + ' - ' + message + ': ' + str(json_output['count']))
                output = json_output['count']
            elif command_type == 'state':
                logger.info(self._props['source_index'] + ' - ' + message + ': ' + json_output['snapshot']['state'])
            else:
                logger.info(self._props['source_index'] + ' - ' + message)
        except (KeyError, TypeError):
            logger.error(self._props['source_index'] + ' - error ' + message + '. response: %s', output)
            raise

        return output

    def compute_properties(self):
        """
        Take in our specially formatted properties file and place the properties as we need them.
        :return: the properties files that can be directly used in replacement
        """
        props = {}

        for key, value in self.load_properties().items():
            if key.startswith(self._CORE):
                self._core_props[key[len(self._CORE):]] = value
                props[value] = None
            elif key.startswith(self._DEF):
                props[key[len(self._DEF):]] = value

        return props

    def load_properties(self, sep='=', comment_char='#', line_end='\\'):
        """
        Read the file passed as parameter as a properties file.
        :return: the properties file as read in
        """
        filename = self.homedir + '/conf/config.properties'

        props = {}
        with open(filename, "rt") as f:
            for line in f:
                l = line.strip()
                if l and not l.startswith(comment_char):
                    key_value = l.split(sep)
                    key = key_value[0].strip()
                    value = sep.join(key_value[1:]).strip().strip('"')
                    props[key] = value

        return props

    def init_logging(self, level=logging.INFO):
        """
        Initialize the logging.
        :param level: level to set logging to
        """
        kw = {
            'format': '[%(asctime)s] %(levelname)s: %(message)s',
            'datefmt': '%Y/%m/%d %H:%M:%S',
            'level': level,
            'stream': sys.stdout
        }
        logging.basicConfig(**kw)

    def set_source_props(self):
        """
        Set the host and index to the source values.
        """
        self._props['host'] = self._props['source_host']
        self._props['index'] = self._props['source_index']
        return self

    def set_dest_props(self):
        """
        Set the host and index to the dest values.
        """
        self._props['host'] = self._props['dest_host']
        self._props['index'] = self._props['dest_index']
        return self

    def set_flux_props(self, index_type, source, dest, date):
        """
        Set the fluctuating properties that change with each pass
        :param index_type:
        :param source:
        :param dest:
        :param date:
        """
        self._props[self._type] = index_type
        self._props[self._source_index] = source
        self._props[self._dest_index] = dest
        self._props[self._date] = date


def usage():
    print('Utility to reindex elasticsearch indices.')
    print('')
    print('\t-d [] or --delay [] \t\tSets the delay between reindex completion and counting of '
          'reindexed docs.\n'
          '\t\t\t\t\tExpected value in seconds.')
    print('\t-e [] or --end [] \t\tSets the end date of the range.  Valid format is yyyy.mm.dd')
    print('\t-h or --help \t\t\tProvides usage help.')
    print('\t-l [] or --log [] \t\tSets the logging level to use while processing.  Defaults to '
          'INFO. Valid values are\n'
          '\t\t\t\t\tERROR, WARN, INFO, DEBUG.')
    print('\t-n \t\t\t\tEnables creating of snapshots for the reindexed indices on the destination '
          'cluster.')
    print('\t-s [] or --start [] \t\tSets the start date of the range.  Valid format is yyyy.mm.dd')


def main(argv):
    """
    Main function.
    :param argv: args to process
    """
    # important starting variables an their defaults
    delay = 10
    end = None
    start = None
    log_level = default_logger_level
    snapshot = False

    # process args
    try:
        opts, args = getopt.getopt(argv, 'd:e:hl:ns:',
                                   ['delay=', 'end=', 'help', 'log=', 'snapshot=', 'start='])
    except getopt.GetoptError:
        usage()
        sys.exit(2)

    for opt, arg in opts:
        if opt in ('-d', '--delay'):
            delay = int(arg)
        if opt in ('-e', '--end'):
            end = str(arg)
        elif opt in ('-h', '--help'):
            usage()
            sys.exit(2)
        elif opt in ('-l', '--log'):
            log_level = str(arg)
        elif opt in ('-n', '--snapshot'):
            snapshot = True
        elif opt in ('-s', '--start'):
            start = str(arg)

    r = Reindexer(log_level)

    if end is None or start is None:
        logger.error('halting processing; both start and end date must be provided.')
        sys.exit(3)

    daterange = r.expand_daterange(start, end)
    logger.info('date range - ' + str(daterange))

    # success flag to see if we finished and can conditionally log
    success = False

    # run reindexing for each date in the range, exit on any failure
    for date in daterange:
        logger.info(str(date) + ' - started processing')
        success = False

        for index_type, item in r.types.items():
            r.set_flux_props(index_type, item.get('source') + date, item.get('dest') + date, date)
            index = item.get('source') + date

            try:
                try:
                    # check if the index exists at the source and skip if it is missing
                    if '404' in r.set_source_props().invoke_template('check_index'):
                        logger.warn(index + ' - skipping; ' + index_type + ' index not available')
                        continue

                    # skip if we have already processed at the destination
                    if '200' in r.set_dest_props().invoke_template('check_index'):
                        logger.warn(index + ' - skipping; ' + index_type + ' already processed')
                        continue

                except (KeyError, TypeError, RuntimeError) as e:
                    logger.error(e)
                    break

                # run count docs query
                before = r.set_source_props().process_command('count_index',
                                           'count of ' + index_type + ' to reindex', 'count')

                # run reindex
                try:
                    logger.info(index + ' - started reindex of ' + index_type + ' with (this may take several minutes)')
                    r.invoke_logstash_template('reindex.conf')
                    logger.info(index + ' - finished reindex of ' + index_type)
                except (KeyError, TypeError, RuntimeError) as e:
                    logger.error(e)
                    break

                # we need a delay because the documents are not be immediately available in elastic
                logger.info(index + ' - pausing for ' + str(delay) + ' seconds')
                time.sleep(delay)

                # run count docs query
                after = r.set_dest_props().process_command('count_index',
                                          'count of ' + index_type + ' reindexed', 'count')

                if before != after:
                    logger.error(index + ' - reindexed ' + index_type + ' before and after counts do not match')
                    raise RuntimeError

                # optionally run create snapshot
                if snapshot:
                    r.process_command('create_snapshot',
                                      'create ' + index_type + ' snapshot', 'state')

                logger.info(index + ' - finished processing index')
                success = True
            except (KeyError, TypeError, RuntimeError) as e:
                logger.error('halting processing; see previous errors')
                break
        logger.info(str(date) + ' - finished processing')
    if success:
        logger.info('date range - finished')


# if I am me, start the main jump program!
if __name__ == '__main__':
    main(sys.argv[1:])
