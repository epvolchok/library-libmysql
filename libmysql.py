from getpass import getpass
from mysql.connector import connect, Error
import pandas as pd
import re
import sys
import atexit

def try_except_decorator(func):
    def wrapper(self, *args, **kwargs):
        try:
            return func(self, *args, **kwargs)
        except Error as er:
            self.close_connection()
            sys.exit(er)
    return wrapper

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
            .display_info(query) - to print simple info like show, describe

    """

    def __init__(self, host_='localhost', database_=''):
        """
        Creates connection to a mysql server or a database if {database_} specified.
        When connecting, it asks for a user name and password.

        """

        self.host = host_
        self.user = input("User name: ")
        self.password = getpass("password: ")
        self.database = database_
        self.connection = None

        try:
            if self.database:
                print(f' Database: {self.database}')
                self.connection = connect(host=self.host, user=self.user,\
                                      password=self.password, database=self.database)
                print(f'You\'ve successfully connected to {self.database}.')
            else:
                self.connection = connect(host=self.host, user=self.user,\
                                      password=self.password)
                print(f'Welcome to {self.host} server.')

            # registers connection to be sure that it will be closed before programm ending
            # even when sys.exit()
            atexit.register(self.close_connection)

        except Error as er:
            sys.exit(er)
        
    def __del__(self):
        """
        Closes connection in the end of the class life-cycle.
        """
        self.close_connection()


    def close_connection(self):
        """
        Explicitly closes the connection to the database.
        """
        if self.connection:
            try:
                self.connection.close()
                print('Connection closed.')
            except Exception as close_error:
                print(f"Error during connection closure: {close_error}")
            self.connection = None  # Deletes a link to the connection object

    @try_except_decorator
    def use_database(self, database):
        """
        Connects to the given database.
        """
        with self.connection.cursor() as cursor:
            self.database = database
            cursor.execute("USE "+database)
            print(f'You\'ve successfully connected to {self.database}.')


    def estimated_query_size(self, query, cursor):
        """
        Estimates maximum size of data fetched by the query.
        Test how to work with joins?
        """
        estimated_size = 0

        # approximately weights the size of one row
        total_size = self._size_of_one_row(query, cursor)
        print(f'Size of one row: {total_size}')
        

        #count the number of rows
        row_count = self._num_of_rows(cursor)
        print(f'Number of rows: {row_count}')

        estimated_size = total_size * row_count

        print(f'Estimated size; {estimated_size}')
        cursor.execute('DROP TABLE temp_table')
        return estimated_size

    # auxiliary functions for estimated_query_size
    # <
    def _field_length(self, column_type):
        num = re.findall(r'\d+', column_type)
        if num:
            return int(num[0])
        else:
            return 1

    @try_except_decorator
    def _size_of_one_row(self, query, cursor):

        create_temp_table_query = """
                CREATE TEMPORARY TABLE temp_table AS
                """
        cursor.execute(create_temp_table_query+query)

        show_columns_query = "SHOW COLUMNS FROM temp_table;"
        cursor.execute(show_columns_query)

        total_size = 0
        columns_info = cursor.fetchall()
        # types of columns and their size

        for column in columns_info:
            column_type = column[1].lower()
            print(column_type)
            if 'int' in column_type:
                length = self._field_length(column_type)
                total_size += 4 * length
            elif 'char' in column_type or 'varchar' in column_type:
                length = self._field_length(column_type)
                total_size += length
            elif 'double' in column_type or 'float' in column_type:
                length = self._field_length(column_type)
                total_size += 8 * length
            elif 'date' in column_type:
                total_size += 3
            elif 'timestamp' in column_type:
                total_size += 4
            elif 'year' in column_type:
                total_size += 1
            elif 'blob' in column_type or 'text' in column_type:
                total_size += 65535 # Maximum size for BLOB or TEXT type

        return total_size

    @try_except_decorator
    def _num_of_rows(self, cursor):
        row_count = 1
        count_query = """
                      SELECT COUNT(*) FROM temp_table;
                      """
        cursor.execute(count_query)
            
        row_count = cursor.fetchone()[0]
        return row_count
    # >

    @try_except_decorator
    def display_query(self, query, param):
        """
        Displays the results of select-queries.
        Two options how to display:
        - print to a terminal (if size of query data < 1 Kbytes)
        - write to a dataframe (if size of query data < 1 Gbytes)
        """
        
        with self.connection.cursor() as cursor:

            if param == 'print':
                self._print_info(query, cursor)
                return 1
            if param == 'dataframe':
                return self._write_to_df(query, cursor)

    # auxiliary functions for display_query
    # <
    def _print_info(self, query, cursor, limit=1024*1024):
        """
        Prints data fetched from a query if its size < {limit} size,
        default limit = 1 Kbytes
        """
        size_query = self.estimated_query_size(query, cursor)
        
        print(f'Approximate query size: {size_query}')
        
        if size_query <= limit:
            cursor.execute(query)
            results = cursor.fetchall()
            for row in results:
                print(row)
        else:
            print('Too much data to print')

    def _write_to_df(self, query, cursor, limit=1024*1024*1024):
        """
        Writes data to a dataframe
        if its size < {limit} size,
        default limit = 1 Gbytes
        """

        
        size_query = self.estimated_query_size(query, cursor)
        print(f'Approximate query size: {size_query}')

        if size_query <= limit:
            cursor.execute(query)
            rows = cursor.fetchall()
            results = pd.DataFrame(rows)
            return results
        else:
            print('Too much data to print')
            return -100
    # >

    @try_except_decorator
    def display_info(self, query):
        """
        Displays simple info like DESCRIBE, SHOW, etc.
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            print(f'{query}:')
            for row in cursor:
                print(row)

    @try_except_decorator
    def execute_query_ch(self, query, params=None):
        """
        Makes changes with a table or a database,
        ends with .commit()
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query, params)
            self.connection.commit()

if __name__ == '__main__':
    conn = db_connection()

    conn.use_database(database='testdb')

    select_query = """
                SELECT *
                FROM test_table
               """


    print('display')
    print('print')
    conn.display_query(select_query, param='print')


    print('dataframe')
    df = conn.display_query(select_query, param='dataframe')
    print(df)
    conn.display_info('DESCRIBE test_table')
    print('alter')
    execute_query = """
            INSERT INTO test_table (name, age, email) VALUES
            (%s, %s, %s)
            """

    data = ('Mark', 25, 'mark@mail.com')
    conn.execute_query_ch(execute_query,  data)
    conn.display_query(select_query, param='print')



