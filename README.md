# to_postgres
to_postgres is a PostGreSQL loader written in python3 that takes input from all sources, cleans the data using Pandas, and pushes the data into PostGreSQL.


```Please install required packages before executing:
       Install mdbtools: ( for MS Access support )
              ubuntu: sudo apt-get install mdbtools
              macOS : brew install mdbtools
       Install all Python3 Packages
              Run: pip3 install -r required_pkgs.txt
              
       
   Be sure to set up your DB connections in the sql_config.py file
```


usage: This tool takes input from one of many sources and creates a PostGreSQL table

       [-h] -ip DATABASE_IP -s SCHEMA -t TABLE [-rt REPLACE_TABLE] -i
       INPUT_TYPE [-st SQL_TABLE] [-sq SQL_QUERY] [-ft FROM_TABLE]
       [-fp FILE_PATH] [-sf SQL_FILE] [-sk SQL_KIND] [-hd HEADERS]
       [-sn SHEET_NUMBER] [-cl COL_LIST [COL_LIST ...]] [-clf COL_LIST_FILE]

optional arguments:
  ```-h, --help            show this help message and exit 
  -ip DATABASE_IP, --database_ip DATABASE_IP
                        The IP of the database the table will be created on.
  -s SCHEMA, --schema SCHEMA
                        The schema the table will be created in (Schema will
                        be created if it does not exitst)
  -t TABLE, --table TABLE
                        The name of the table to be created.
  -rt REPLACE_TABLE, --replace_table REPLACE_TABLE
                        If 'true', if a table of the same name exists, the
                        table will be dropped and replaced.
  -i INPUT_TYPE, --input_type INPUT_TYPE
                        Available Types: XL (Excel File), CSV (CSV File), TAB
                        (Tab Delimited Text File), JSON (JSON Text File), SQLT
                        (SQL table), SQLQ (SQL Query)
  -st SQL_TABLE, --sql_table SQL_TABLE
                        The SQL table to be read when using SQLT
  -sq SQL_QUERY, --sql_query SQL_QUERY
                        The SQL query to be read when using SQLQ
  -ft FROM_TABLE, --from_table FROM_TABLE
                        The target input SQL Table when using --input_type
                        SQLT
  -fp FILE_PATH, --file_path FILE_PATH
                        The File Path for input_types: XL, CSV, TAB, JSON, and
                        file based SQL Databases
  -sf SQL_FILE, --sql_file SQL_FILE
                        Pass 'true' if SQLT or SQLQ will use file based SQL
  -sk SQL_KIND, --sql_kind SQL_KIND
                        Kind of SQL connection to use when obtaining data:
                        PSQL (PostGreSQL), MSQL (MySQL), MDB (Access DB), SQLL
                        (SQLite)
  -hd HEADERS, --headers HEADERS
                        Pass 'false' if CSV or TAB file does not contain
                        column headers.
  -sn SHEET_NUMBER, --sheet_number SHEET_NUMBER
                        Pass the sheet number index of a multi-sheet Excel
                        file to be uploaded (starting from 0)
  -cl COL_LIST [COL_LIST ...], --col_list COL_LIST [COL_LIST ...]
                        Supply an ordered list of column headers when -hd is
                        set to false
  -clf COL_LIST_FILE, --col_list_file COL_LIST_FILE
                        Supply a file path to a file containing ordered column
                        headers separated by whitespace when -hd is set to
                        false
                        ```
