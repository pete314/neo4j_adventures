#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""
Author: Peter Nagy
Since: 2016-02-22
Description: Check if file is accessible for usage, download if not
"""

import urllib2
import StringIO
import os.path
import csv
from zipfile import ZipFile


class FileResolver(object):
    def __init__(self, filename="", file_url=""):
        """
        Init the vars required
        :param filename: local file path, default top-1m.csv.zip
        :param fil_eurl: url to internet location, default http://s3.amazonaws.com/alexa-static/top-1m.csv.zip
        :return: null
        """
        self.filename = filename
        self.file_url = file_url
        self.default_url = 'http://s3.amazonaws.com/alexa-static/top-1m.csv.zip'

    def __call__(self, filename, limit):
        self.filename = filename
        return self.get_parsed_zip_csv(limit)

    def download_file(self):
        """
        Download the file from the internet and store in memory
        :return: local file path
        """

        try:
            # response = urllib2.urlopen(self.default_url) if self.file_url == "" else urllib2.urlopen(self.file_url)
            response = urllib2.urlopen(self.default_url)
        except urllib2.HTTPError, e:
            print ("There was an error with download, error: %s" % e)
            return False
        else:
            return response.read()

    def get_file_stream(self):
        """
        Check if local file is available, if not download
        :return: StringIO - File stream
        """
        if self.filename == "":
            stream = self.download_file()
            return stream if stream else False
        else:
            if os.path.isfile(self.filename):
                return file.read(file(self.filename))
            else:
                print ("File passed does not exist, downloading default.\\n")
                self.filename = ""
                self.get_file_stream()

    def get_parsed_zip_csv(self, limit=1000000):
        """
        Parse zip file extract fields from data
        NOTE: this is specific function for top-1m.csv.zip file
        :return: dict
        """
        with ZipFile(self.filename) as zf:
            csv_filename = zf.namelist()[0]
            cnt = 0
            tmp_values = []
            for _, site in csv.reader(zf.open(csv_filename)):
                uri_bits = site.split('.')
                tmp_values.append({'url': ('http://' + site), 'brand': uri_bits[0], 'tld': uri_bits[1], 'website': site})

                cnt += 1
                if limit == cnt:
                    return tmp_values

            return tmp_values
        return False

