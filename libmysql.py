from getpass import getpass
from mysql.connector import connect, Error
import pandas as pd
from sys import getsizeof
import numpy as np


class db_connection():
    """
        Connects to the (local, default) mysql-server
        using user name and password entered from keyboard
        to create and change databases/tables, write queries.
            __init__ - creates connection:
            __del__ - closes the connection
            .close_connection() - closes connection manually
            .execute_query_ch(query) - for queries making changes,
            ends with commit
            .display_query(query) - for showing some info queries

    """
    def __init__(self, host_='localhost', database_=''):
        """
        Creates connection to mysql server or a database if {database_} specified.
        When connecting, it asks for a user name and password.
.
        """

        self.host = host_
        self.user = input("User name: ")
        self.password = getpass("password: ")
        self.database = database_

        try:
            if self.database:
                print(f' Database: {self.database}')
                self.connection = connect(host=self.host, user=self.user,\
                                      password=self.password, database=self.database)
            else:
                self.connection = connect(host=self.host, user=self.user,\
                                      password=self.password)
                print('Welcome to {self.host} server.')
            
        except Error as er:
            print(er)
        
    def __del__(self):
        """
        Closes connection in the end of its life-cycle.
        """
        self.connection.close()
        print('Connection closed')

    def close_connection(self):
        """
        Closes connection manually.
        """
        self.connection.close()
        print('Connection closed')


    def estimated_query_size(self, query, cursor):
        """
        Estimates approximate size of data fetched by query.
        Test how to work with joins. (wrong)
        """
        #count number of rows
        try:
            count_query = f"SELECT COUNT(*) FROM ({query}) AS count_query"
            cursor.execute(count_query)
            row_count = cursor.fetchone()[0]
            print(f'rows_num: {row_count}')
        except Error as er:
            print(er)
    
        #count number of columns
        try:
            base_table = query.split('FROM')[1].split()[0]  # Extract the table name from the query
            cursor.execute(f"DESCRIBE {base_table}")
            columns = cursor.fetchall()
            print(f'columns_num: {len(columns)}')
        except Error as er:
            print(er)
    
    
        total_size = 0
        for column in columns:
            column_type = column[1].lower()
            if 'int' in column_type:
                total_size += 4
            elif 'char' in column_type or 'text' in column_type:
                length = int(column_type.split('(')[1].split(')')[0])
                total_size += length
            elif 'double' in column_type or 'float' in column_type:
                total_size += 8
            elif 'date' in column_type:
                total_size += 3
            elif 'timestamp' in column_type:
                total_size += 4
            elif 'year' in column_type:
                total_size += 1
            elif 'blob' in column_type:
                length = int(column_type.split('(')[1].split(')')[0])
                total_size += length
    
        estimated_size = total_size * row_count
    
        return estimated_size

    def print_info(self, query, cursor, limit=1024*1024):
        """
        Prints data fetched from a query if its size < {limit} size,
        default limit = 1 Mbytes
        """
        results = cursor.fetchall()
        size_query = self.estimated_query_size(query, cursor)
        
        #for row in results:
        #   print(row)
        if size_query < limit:
            for row in results:
                print(row)
        else:
            print('Too much data to print')

    def display_query(self, query, param):
        """
        Displays the results of select-queries.
        Two options how to display:
        - print to a terminal (if size of query data < 1 Mbytes)
        - write to a dataframe (if size of query data < 10 Gbytes)(not implemented)
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                if param == 'print':
                    self.print_info(query, cursor)
        except Error as er:
            print(er)

    def display_info(self, query):
        """
        Displays simple info like DESCRIBE, SHOW, etc.
        """
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                print(f'{query}:')
                for row in cursor:
                    print(row)
        except Error as er:
            print(er)

    def execute_query_ch(self, query):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                self.connection.commit()
        except Error as er:
            print(er)


conn = db_connection(database_='gergs_list')
select_query = """
                SELECT *
                FROM doughnut_list
               """


print('merged?')
conn.display_query(select_query, param='print')
conn.display_info('DESCRIBE doughnut_list')


