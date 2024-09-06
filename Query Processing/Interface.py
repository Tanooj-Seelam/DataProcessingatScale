#!/usr/bin/python2.7
#
# Tanooj's interface 
#

import psycopg2
import os
import sys


# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratings_table_name, min_rating, max_rating, open_connection):
    result = []
    cursor = open_connection.cursor()

    partition_query = '''SELECT partitionnum FROM rangeratingsmetadata WHERE maxrating>={0} AND minrating<={1};'''.format(
        min_rating, max_rating)
    cursor.execute(partition_query)
    partitions = [partition[0] for partition in cursor.fetchall()]

    select_range_query = '''SELECT * FROM rangeratingspart{0} WHERE rating>={1} and rating<={2};'''

    for partition in partitions:
        cursor.execute(select_range_query.format(partition, min_rating, max_rating))
        sql_result = cursor.fetchall()
        for res_1 in sql_result:
            res_1 = list(res_1)
            res_1.insert(0, 'RangeRatingsPart{}'.format(partition))
            result.append(res_1)

    count_rr_query = 'SELECT partition_num FROM roundrobinratingsmetadata;'
    cursor.execute(count_rr_query)
    rr_parts = cursor.fetchall()[0][0]

    select_rr_query = '''SELECT * FROM roundrobinratingspart{0} WHERE rating >= {1} AND rating <= {2};'''

    for i in range(0, rr_parts):
        cursor.execute(select_rr_query.format(i, min_rating, max_rating))
        sql_result = cursor.fetchall()
        for res_2 in sql_result:
            res_2 = list(res_2)
            res_2.insert(0, 'RangeRatingsPart{}'.format(i))
            result.append(res_2)

    write_to_file('RangeQueryOut.txt', result)


def PointQuery(ratings_table_name, rating_value, open_connection):
    result = []
    cursor = open_connection.cursor()

    partition_query = '''SELECT partition_num FROM rangeratingsmetadata WHERE max_rating >= {0} AND min_rating <= {0};'''.format(
        rating_value)
    cursor.execute(partition_query)
    partitions = [partition[0] for partition in cursor.fetchall()]

    select_range_query = '''SELECT * FROM rangeratingspart{0} WHERE rating = {1};'''

    for partition in partitions:
        cursor.execute(select_range_query.format(partition, rating_value, ))
        sql_result = cursor.fetchall()
        for res_3 in sql_result:
            res_3 = list(res_3)
            res_3.insert(0, 'RangeRatingsPart{}'.format(partition))
            result.append(res_3)

    count_rr_query = 'SELECT partition_num FROM roundrobinratingsmetadata;'
    cursor.execute(count_rr_query)
    rr_parts = cursor.fetchall()[0][0]

    select_rr_query = '''SELECT * FROM roundrobinratingspart{0} WHERE rating = {1};'''

    for i in range(0, rr_parts):
        cursor.execute(select_rr_query.format(i, rating_value, ))
        sql_result = cursor.fetchall()
        for res_4 in sql_result:
            res_4 = list(res_4)
            res_4.insert(0, 'RangeRatingsPart{}'.format(i))
            result.append(res_4)

    write_to_file('PointQueryOut.txt', result)


def write_to_file(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()
