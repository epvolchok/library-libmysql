# **libmysql** &mdash; library for faster and simpler interaction with a mysql database
The library simplifies interaction with a database by mysql.connector.
Class db_connection connects to the (local, default) mysql-server using a user name and password entered from keyboard
to create and change databases/tables, write queries. When program ends the connection is closed automatically.
Methods:
- **\__init\__**(host_='localhost', database_='') &ndash; creates connection.
- **\__del\__** &ndash; closes the connection.
- **.close_connection**() &ndash; closes the connection manually.
- **.execute_query_ch**(query) &ndash; for queries making changes, ends with commit.
- **.display_query**(query, param, estsize=False, limit=None) &ndash; for showing results of queries, writes to a terminal or pandas dataframe.
  If estsize is True it compares the estimated size of the query with a limit before printing.
- **.display_info**(query) &ndash; to print simple info like SHOW, DESCRIBE.
- **.use_database**(database) &ndash; to choose or change a database.

The class has a built-in check for the correct connection to the server and execution of requests.
If an error occurs, the program (only the simplest case is implemented so far) terminates with an error message.

Modules used: mysql.connector, getpass, pandas, re, sys, atexit.
