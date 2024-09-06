#!/usr/bin/python2.7
#
# Tanooj's interface
#

import psycopg2
from io import StringIO
from itertools import islice


def getOpenConnection(db_user='postgres', db_password='1234', db_name='postgres'):
    return psycopg2.connect(f"dbname='{db_name}' user='{db_user}' host='localhost' password='{db_password}'")


def loadRatings(rating_table_name, ratings_file_path, open_connection):
    cursor = open_connection.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {rating_table_name}")
    cursor.execute(f"CREATE TABLE {rating_table_name} (user_id INT not null, movie_id INT, rating FLOAT, timestamp INT)")

    with open(ratings_file_path) as f:
        for n_lines in iter(lambda: tuple(islice(f, 5000)), ()):
            batch = StringIO()
            batch.write(''.join(line.replace('::', ',') for line in n_lines))
            batch.seek(0)
            cursor.copy_from(batch, rating_table_name, sep=',', columns=('user_id', 'movie_id', 'rating', 'timestamp'))

    cursor.execute(f"ALTER TABLE {rating_table_name} DROP COLUMN timestamp")
    open_connection.commit()
    cursor.close()


def rangePartition(rating_table_name, number_of_partitions, open_connection):
    parts = 5.0 / number_of_partitions
    cursor = open_connection.cursor()

    for i in range(number_of_partitions):
        if i == 0:
            cursor.execute(
                f"CREATE TABLE range_partition_{i} AS SELECT * FROM {rating_table_name} WHERE rating >= {i * parts} AND rating <= {(i + 1) * parts}")
        else:
            cursor.execute(
                f"CREATE TABLE range_partition_{i} AS SELECT * FROM {rating_table_name} WHERE rating > {i * parts} AND rating <= {(i + 1) * parts}")
        open_connection.commit()

    cursor.close()


def roundRobinPartition(rating_table_name, number_of_partitions, open_connection):
    create_query = '''
    CREATE TABLE rrobin_partition_{0} AS 
    SELECT user_id, movie_id, rating 
    FROM (SELECT user_id, movie_id, rating, ROW_NUMBER() OVER() as rowid FROM {1}) AS temp
    WHERE mod(temp.rowid-1, {2}) = {3}
    '''

    cursor = open_connection.cursor()

    for i in range(number_of_partitions):
        cursor.execute(create_query.format(i, rating_table_name, number_of_partitions, i))
        open_connection.commit()

    cursor.close()


def roundRobinInsert(rating_table_name, user_id, movie_id, rating, open_connection):
    cursor = open_connection.cursor()
    insert_query = f"INSERT INTO {rating_table_name} VALUES ({user_id}, {movie_id}, {rating})"
    cursor.execute(insert_query)

    cursor.execute(f"SELECT * FROM {rating_table_name}")
    num_records = len(cursor.fetchall())

    cursor.execute("SELECT * FROM information_schema.tables WHERE table_name LIKE 'rrobin_partition_%'")
    num_parts = len(cursor.fetchall())

    hash_val = (num_records - 1) % num_parts
    insert_robin_query = f"INSERT INTO rrobin_partition_{hash_val} VALUES ({user_id}, {movie_id}, {rating})"
    cursor.execute(insert_robin_query)
    open_connection.commit()
    cursor.close()


def rangeInsert(rating_table_name, user_id, movie_id, rating, open_connection):
    cursor = open_connection.cursor()
    insert_query = f"INSERT INTO {rating_table_name} VALUES ({user_id}, {movie_id}, {rating})"
    cursor.execute(insert_query)

    cursor.execute("SELECT * FROM information_schema.tables WHERE table_name LIKE 'range_partition_%'")
    num_parts = len(cursor.fetchall())

    insert_range_query = "INSERT INTO range_partition_{0} VALUES ({1}, {2}, {3})"
    parts = 5.0 / num_parts

    for i in range(num_parts):
        if i == 0:
            if rating >= i * parts and rating <= (i + 1) * parts:
                cursor.execute(insert_range_query.format(i, user_id, movie_id, rating))
        else:
            if rating > i * parts and rating <= (i + 1) * parts:
                cursor.execute(insert_range_query.format(i, user_id, movie_id, rating))
    open_connection.commit()
    cursor.close()


def createDB(db_name='dds_assignment'):
    con = getOpenConnection(db_user='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    cur.execute(f"SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname='{db_name}'")
    count = cur.fetchone()[0]

    if count == 0:
        cur.execute(f"CREATE DATABASE {db_name}")
    else:
        print(f'A database named {db_name} already exists')

    cur.close()
    con.close()


def Delete_Partitions(open_connection):
    cur = open_connection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")

    l = []
    for row in cur:
        l.append(row[0])
    for table_name in l:
        cur.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")

    cur.close()


def deleteTables(ratings_tname, open_conn):
    try:
        cursor = open_conn.cursor()
        if ratings_tname.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            table_names = [table_name[0] for table_name in cursor.fetchall()]
            for table_name in table_names:
                cursor.execute(f"DROP TABLE {table_name} CASCADE")
        else:
            cursor.execute(f"DROP TABLE {ratings_tname} CASCADE")
        open_conn.commit()
    except psycopg2.DatabaseError as e:
        if open_conn:
            open_conn.rollback()
        print(f'Error {e}')
    except IOError as e:
        if open_conn:
            open_conn.rollback()
        print(f'Error {e}')
    finally:
        if cursor:
            cursor.close()
