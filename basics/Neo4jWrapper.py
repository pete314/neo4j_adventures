#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""
Author: Peter Nagy
Since: 2016-02-22
Description: Wrapper for Neo4j functions
"""

from py2neo import Graph
from py2neo import neo4j
import sys

class Neo4jWrapper(object):

    def __init__(self, user_name, password, connection_string="", current_id=1):
        self.connection_string = connection_string \
                    if connection_string != "" \
                    else "http://"+ user_name+":"+password+"@localhost:7474/db/data/"
        self.graph_db = Graph(self.connection_string)
        self.current_id = current_id

    def delete_all_nodes(self):
        self.graph_db.delete_all()

    def insert_single_node(self, node_map):
        """
        Insert pre specified node type
        :param node_map:  format {'url':, 'brand': , 'tld': , 'website':}
        :return:
        """
        self.graph_db.cypher.execute("CREATE (w:Websites {id:{I}, brand:{BRAND}, website:{W}, url:{U}, tld:{TLD}})",
                  {"BRAND": node_map['brand'], "I": self.current_id, "W": node_map['website'],
                   "U": node_map['url'], "TLD": node_map['tld']})
        self.current_id += 1

    def insert_as_transaction(self, node_map_list):
        """
        Insert nodes with transaction
        :param node_map_list: list of data in format {'url':, 'brand': , 'tld': , 'website':}
        :return: None
        """
        tx = self.graph_db.cypher.begin()
        for node_map in node_map_list:
            self.current_id += 1
            tx.append("CREATE (w:Websites {id:{I}, brand:{B}, website:{W}, url:{U}, tld:{T}})", {
                "B": node_map['brand'], "I": self.current_id, "W": node_map['website'],
                "U": node_map['url'], "T": node_map['tld']})

        tx.commit()

    def batch_create(self, node_map_list):
        """ Execute multiple insert as batch jobs
        :param node_map_list:
        """
        batch = neo4j.WriteBatch(self.graph_db)
        for node_map in node_map_list:
            self.current_id += 1
            batch.append(self.create_cypher_job(None, node_map))
        return batch.submit()

    def create_cypher_job(self, statement=None, params_dict=dict()):
        """
        Create cypher job for bach insert
        :param statement:
        :param params_dict:
        :return: neo4j.CypherJob
        """
        default_statement = "CREATE (w:Websites {id:{I}, brand:{B}, website:{W}, url:{U}, tld:{T}})"
        default_params = {"B": params_dict['brand'], "I": self.current_id, "W": params_dict['website'],
                          "U": params_dict['url'], "T": params_dict['tld']}

        if statement is None or len(params_dict) is 0:
            return neo4j.CypherJob(default_statement, default_params)
        else:
            return neo4j.CypherJob(statement, params_dict)

    def insert_single_with_loop(self, node_map_list, insertion_type="single", insertion_size=0):
        """
        Convenient method to loop on data list
        :param node_map_list: list for dict in format {'url':, 'brand': , 'tld': , 'website':}
        :param insertion_type: single | transaction | batch
        :param insertion_size: integer - the number of nodes inserted in a single transaction
        :return: None
        """
        cnt = 0
        temp_node_holder = []
        for node_map in node_map_list:
            if insertion_type is 'single':
                self.insert_single_node(node_map)
            elif insertion_type is 'transaction' or 'batch':
                cnt += 1
                if cnt % insertion_size is 0:
                    self.insert_as_transaction(temp_node_holder) \
                        if insertion_type is 'transaction' \
                        else self.batch_create(temp_node_holder)
                    # replace line instead new line
                    sys.stdout.write("\r++++INSERTED %d++++" % cnt)
                    sys.stdout.flush()
                    temp_node_holder = []
                else:
                    temp_node_holder.append(node_map)

        # insert any remaining
        if insertion_type is 'transaction' or 'batch':
            self.insert_as_transaction(temp_node_holder) \
                        if insertion_type is 'transaction' \
                        else self.batch_create(temp_node_holder)
