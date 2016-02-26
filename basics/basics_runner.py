#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""
Author: Peter Nagy
Since: 2016-02-22
Description: Glue file
"""

import time
import os
import psutil
from FileResolver import FileResolver
from Neo4jWrapper import Neo4jWrapper

start_time = time.time()

def get_update_last_timestamp(last_timestamp, message=""):
    print("Time elapsed: %s seconds\n%s\n=========="
          % (time.time() - last_timestamp, message))
    return time.time()


def get_file_data(insert_limit):
    """
    Prepare data for insertion
    :param insert_limit: The data size to use
    :return: Strunctured data or false if error
    """
    last_timestamp = time.time()
    # Read formatted data from csv file
    file_resolver = FileResolver(get_file_from_data_dir())
    data = file_resolver.get_parsed_zip_csv(insert_limit)
    get_update_last_timestamp(last_timestamp, 'Data parsed from file')

    return data


def insert_single_rows(insert_limit=1000000):
    """
    Insert nodes one by one
    :param insert_limit: The number of nodes to insert
    :return:
    """
    last_timestamp = time.time()
    data = get_file_data(insert_limit)

    # Create new instance of NeoWrapper and clean database
    neo_wrapper = Neo4jWrapper('neo4j', 'neo4j')
    neo_wrapper.delete_all_nodes()
    last_timestamp = get_update_last_timestamp(last_timestamp, "All nodes deleted")

    # Insert the data into Neo4j
    starting_memory_profile = psutil.virtual_memory()
    neo_wrapper.insert_single_with_loop(data)
    finishing_memory_profile = psutil.virtual_memory()
    last_timestamp = get_update_last_timestamp(last_timestamp,
                        "All nodes inserted"
                        + "\nStarting (free) memory: " + (str)(starting_memory_profile.free / 1024) +"KB"
                        + "\nCurrent (free)memory: " + (str)(finishing_memory_profile.free / 1024) +"KB"
                        + "\nTotal memory used: " + (str)((finishing_memory_profile.used - starting_memory_profile.used)/1024)+"KB")


def insert_with_trasactions(insert_limit=1000000):
    """
    Insert into db with transactions

    :param insert_limit: The number of nodes to insert
    :return:
    """
    last_timestamp = time.time()
    data = get_file_data(insert_limit)

    # Create new instance of NeoWrapper and clean database
    neo_wrapper = Neo4jWrapper('neo4j', 'Admin1234!')
    neo_wrapper.delete_all_nodes()
    last_timestamp = get_update_last_timestamp(last_timestamp, "All nodes deleted")

    # Insert the data into Neo4j
    starting_memory_profile = psutil.virtual_memory()
    neo_wrapper.insert_single_with_loop(data, 'transaction', 500)
    finishing_memory_profile = psutil.virtual_memory()
    last_timestamp = get_update_last_timestamp(last_timestamp,
                        "All nodes inserted"
                        + "\nStarting (free) memory: " + (str)(starting_memory_profile.free / 1024) +"KB"
                        + "\nCurrent (free)memory: " + (str)(finishing_memory_profile.free / 1024) +"KB"
                        + "\nTotal memory used: " + (str)((finishing_memory_profile.used - starting_memory_profile.used)/1024)+"KB")


def get_file_from_data_dir(filename="top-1m.csv.zip"):
    basepath = os.path.dirname(__file__)
    return os.path.abspath(os.path.join(basepath, "..", "data", filename))

if __name__ == '__main__':
    # delete_current_nodes()
    #insert_single_rows(100)
    insert_with_trasactions(5000)
    # print ("%d items inserted in %s seconds" % (node_cnt, time.time() - start_time))
