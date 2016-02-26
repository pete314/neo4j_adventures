#!/usr/bin/env python
# -*- encoding: utf-8 -*-

"""
Author: Peter Nagy
Since: 2016-02-22
Description: Wrapper for Neo4j functions
"""

from py2neo import Graph, Path


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
            self.current_id +=1
            tx.append("CREATE (w:Websites {id:{I}, brand:{BRAND}, website:{W}, url:{U}, tld:{TLD}})",
                  {"BRAND": node_map['brand'], "I": self.current_id, "W": node_map['website'],
                   "U": node_map['url'], "TLD": node_map['tld']})

        tx.commit()

    def insert_single_with_loop(self, node_map_list, insertion_type="single", insertion_size=0):
        """
        Convenient method to loop on data list
        :param node_map_list: list for dict in format {'url':, 'brand': , 'tld': , 'website':}
        :param insertion_type: single | transaction | batch
        :param insertion_size: integer - the number of nodes inserted in a single transaction
        :return: None
        """
        cnt = 0
        transaction_node_holder = []
        for node_map in node_map_list:
            if insertion_type is 'single':
                self.insert_single_node(node_map)
            elif insertion_type is 'transaction':
                cnt += 1
                if cnt == insertion_size:
                    self.insert_as_transaction(transaction_node_holder)
                else:
                    transaction_node_holder.append(node_map)
                    transaction_node_holder = []

        #insert any remaining
        if insertion_type is 'transaction':
            self.insert_as_transaction(transaction_node_holder)
