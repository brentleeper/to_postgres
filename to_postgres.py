#!/usr/bin/env python3
import sys
from shutil import copyfile
import traceback
import io
import argparse
import pandas as pd
import numpy as np
import psycopg2 as pspg
from sqlalchemy import create_engine
import dbf
import sqlite3
import mysql
import pandas_access as mdb
from sql_config import psql_target_conn_str, psql_source_conn_str, mysql_source_conn_str

if psql_target_conn_str is None:
	print("Please set up the PSQL target connection in the sql_config.py file.")
	sys.exit()

parser = argparse.ArgumentParser("This tool takes input from one of many sources and creates a PostGreSQL table\n")

parser.add_argument("-ip", "--database_ip", required=True, help="The IP of the database the table will be created on.")
parser.add_argument("-s", "--schema", required=True, help="The schema the table will be created in (Schema will be created if it does not exitst)")
parser.add_argument("-t", "--table", required=True, help="The name of the table to be created.")
parser.add_argument("-rt", "--replace_table", required=False, help="If 'true', if a table of the same name exists, the table will be dropped and replaced.")
parser.add_argument("-i", "--input_type", required=True, help="Available Types: XL (Excel File), CSV (CSV File), TAB (Tab Delimited Text File), JSON (JSON Text File), SQLT (SQL table), SQLQ (SQL Query)")
parser.add_argument("-st", "--sql_table", required=False, help="The SQL table to be read when using SQLT")
parser.add_argument("-sq", "--sql_query", required=False, help="The SQL query to be read when using SQLQ")
parser.add_argument("-ft", "--from_table", required=False, help="The target input SQL Table when using --input_type SQLT")
parser.add_argument("-fp", "--file_path", required=False, help="The File Path for input_types: XL, CSV, TAB, JSON, and file based SQL Databases")
parser.add_argument("-sf", "--sql_file", required=False, help="Pass 'true' if SQLT or SQLQ will use file based SQL")
parser.add_argument("-sk", "--sql_kind", required=False, help="Kind of SQL connection to use when obtaining data: PSQL (PostGreSQL), MSQL (MySQL), MDB (Access DB), SQLL (SQLite)")
parser.add_argument("-hd", "--headers", required=False, help="Pass 'false' if CSV or TAB file does not contain column headers.")
parser.add_argument("-sn", "--sheet_number", required=False, help="Pass the sheet number index of a multi-sheet Excel file to be uploaded (starting from 0)")
parser.add_argument("-cl", "--col_list", nargs='+', required=False, help="Supply an ordered list of column headers when -hd is set to false")
parser.add_argument("-clf", "--col_list_file", required=False, help="Supply a file path to a file containing ordered column headers separated by whitespace when -hd is set to false")


argss = parser.parse_args()


missing_values = ["", "-", " ", "--", "n/a", "na", "N/A", "NA", "Na", "N/a", "n/A", "null", "NULL", "Null", "none", "NONE", "None", "nill", "NILL", "Nill"]

def push_to_psql(args, df):
    #psql_engine = create_engine("postgresql+psycopg2://welltraveled:group5@74.113.215.53:5432/welltraveled")
    psql_engine = create_engine(psql_target_conn_str)
    psql_conn = psql_engine.raw_connection()

    check_schema = psql_conn.cursor()

    cleaned_schema = args.schema.split()[0].split(";")[0]

    check_schema_stmt = "SELECT count(*) FROM information_schema.schemata WHERE schema_name = '" + cleaned_schema + "';"

    check_schema.execute(check_schema_stmt)

    schema_exists = check_schema.fetchone()[0] == 1

    if not schema_exists:
        print(f"Schema Not Found: {args.schema}")
        create_schema(args.schema, psql_conn)
        print("Schema Created")
    else:
        print(f"Schema Exists: {args.schema}")

    if args.replace_table.__str__().lower() == "true":
        replace_table = "replace"
    else:
        replace_table = "fail"

    print("Cleaning Data...")


    bad_data_path = "bad_data_" + args.table + ".log"

    bad_data_file = open(bad_data_path, "w")

    for col in df.columns:      #remove leading or trailing whitespace chars from data in each column
        try:
            df[col] = df[col].apply(lambda x: x.strip())
        except:
            pass

        if df[col].dtype == "object":
            for index, val in enumerate(df[col]):
                if type(val) == type("str") and "\r" in val:
                    print("---------------------------------------------------------------------------------------------")
                    bad_data_file.write("---------------------------------------------------------------------------------------------\n")
                    print("Skipping line " + str(index) + " due to embeded carriage return in column '" + col + "':\n")
                    bad_data_file.write("Skipping line " + str(index) + " due to embeded carriage return in column '" + col + "':\n\n")
                    print(df.iloc[index])
                    bad_data_file.write(str(df.iloc[index]))

                    df = df.drop(df.index[index])

    print()

    print("Skipped rows written to: " + bad_data_path)
    bad_data_file.close()

    columns = df.columns

    cleaned_columns = []

    for col in columns:
        cleaned_columns.append(col.strip().lower().replace(" ", "_"))

    df.columns = cleaned_columns

    try:
        print(f"Creating Table: {args.schema}.{args.table}")

        df.head(0).to_sql(args.table, psql_engine, schema=args.schema , if_exists=replace_table, index=False)
    except:
        print("TABLE ALREADY EXISTS: Please specify --replace_table (-rt) True in order to replace the exitsting table.")
        sys.exit()

    create_table = psql_conn.cursor()

    output = io.StringIO()

    df.to_csv(output, sep='\t', header=False, index=False)

    output.seek(0)

    contents = output.getvalue()

    try:
        print("Copying Data")
        create_table.copy_from(output, args.schema + "." + args.table, null="")
        psql_conn.commit()
    except:
        traceback.print_exc()
        sys.exit()



def create_schema(schema, psql_conn):
    make_schema = psql_conn.cursor()
    make_schema_stmt = "CREATE SCHEMA " + schema + ";"
    # make_schema_stmt = "CREATE SCHEMA test AUTHORIZATION taxnet;"

    make_schema.execute(make_schema_stmt)

    psql_conn.commit()


def get_dataframe_from_input(args):
    input_type = args.input_type

    database_ip = args.database_ip
    schema = args.schema
    table = args.table
    is_sql_file = args.sql_file.__str__().lower() == 'true'

    print("Reading input...")

    if input_type.upper() == "XL":
        df = from_excel(args)
    elif input_type.upper() == "CSV":
        df = from_CSV(args)
    elif input_type.upper() == "TAB":
        df = from_TAB(args)
    elif input_type.upper() == "JSON":
        df = from_JSON(args)
    elif input_type.upper() == "SQLT":
        df = from_SQLT(args)
    elif input_type.upper() == "SQLQ":
        df = from_SQLQ(args)
    else:
        df = f"""UNSUPPORTED input_type: {input_type}\nAvailable Types: 
        		 XL (Excel File), CSV (CSV File), TAB (Tab Delimited Text 
        		 File), JSON (JSON Text File), SQLT (SQL table), SQLQ (SQL Query)"""

    return df


def from_excel(args):

    if args.file_path is not None:
        try:
            xl_file = pd.ExcelFile(args.file_path)
        except:
            return "File does not match input_type"
        if len(xl_file.sheet_names) == 1:
            try:
                df = pd.read_excel(args.file_path)
            except FileNotFoundError:
                df = f"FILE NOT FOUND: {args.file_path}"
            except:
                df = traceback.format_exc()
        elif args.sheet_number is not None:
            try:
                df = pd.read_excel(xl_file, xl_file.sheet_names[int(args.sheet_number)])
            except:
                df = f"Invalid sheet_number: {args.sheet_number} -> possible options: {0} - {len(xl_file.sheet_names) - 1}"
        else:
            df = f"Multiple Sheets in File: {args.file_path} -> Sheets: {pd.ExcelFile(args.file_path).sheet_names} Use --sheet_number (-sn) to specify sheet index"
    else:
        df = "Must Supply --file_path (-fp) of input file."

    return df


def handle_headers(args):
    try:
        if args.headers is None or not args.headers.lower() == "false":
            headers = None
        elif args.col_list is not None:
            headers = args.col_list
        elif args.col_list_file is not None:
            try:
                col_file = open(args.col_list_file)
            except FileNotFoundError:
                print(f"Column List File Not Found: {args.col_list_file}")
                sys.exit()
            except:
                traceback.print_exc()
                sys.exit()
            else:
                contents = col_file.read()
                headers = contents.lower().strip().split()
        else:
            headers = None
            print("No Column Headers. Use -cl to supply list of column headers or -clf to pass path to file containing headers when -hd is 'false'")
            sys.exit()

        if headers is not None:
            add_headers(args, headers)
    except:
        traceback.print_exc()
        sys.exit()

def add_headers(args, header_list):
    if args.input_type.upper() == "CSV":
        header_line = ','.join(header_list)
    else:
        header_line = '\t'.join(header_list)

    copyfile(args.file_path, args.file_path+"_save")

    with open(args.file_path, "r+") as prepend_file:
        content = prepend_file.read()
        prepend_file.seek(0, 0)
        prepend_file.write(","+header_line + '\n')




def from_CSV(args):

    if args.file_path is not None:
        handle_headers(args)

        try:
            df = pd.read_csv(args.file_path, low_memory=False, na_values=missing_values)
        except FileNotFoundError:
            df = f"FILE NOT FOUND: {args.file_path}"
        except:
            df = traceback.format_exc()
            error_line_no = int(df.strip().split("\n")[-1].split("line ")[-1].split(",")[0])

            with open(args.file_path) as cur_file:
                for i, line in enumerate(cur_file):
                    if i + 1 == error_line_no:
                        df += "Error Line:\n" + line
                        break
    else:
        df = "Must Supply --file_path (-fp) of input file."

    return df


def from_TAB(args):

    if args.file_path is not None:

        handle_headers(args)

        try:
            df = pd.read_csv(args.file_path, sep="\t", low_memory=False, na_values=missing_values)
        except FileNotFoundError:
            df = f"FILE NOT FOUND: {args.file_path}"
        except:
            df = traceback.format_exc()
            error_line_no = int(df.strip().split("\n")[-1].split("line ")[-1].split(",")[0])

            with open(args.file_path) as cur_file:
                for i, line in enumerate(cur_file):
                    if i + 1 == error_line_no:
                        df += "Error Line:\n" + line
                        break
    else:
        df = "Must Supply --file_path (-fp) of input file."

    return df


def from_JSON(args):
    return "JSON Not Currently Supported... :("


def from_SQLT(args):
    from_table = args.from_table

    if from_table == None or from_excel == "":
        return "MUST SUPPLY --from_table"

    sql_type = args.sql_kind.lower()

    if sql_type == "psql":
        return sql_type + " Coming Soon"
    elif sql_type == "msql":
        return sql_type + " Coming Soon"
    elif sql_type == "sqll":
        return sql_type + " Coming Soon"
    elif sql_type == "mdb":
        sql_file = args.sql_file

        if sql_file == None or sql_file == "":
            return "MUST SUPPLY --sql_file"

        try:
            tables = mdb.list_tables(sql_file)

            if from_table not in tables:
                return "TABLE NOT FOUND. Available Tables:\n\t" + "\n\t".join(tables)

            df = mdb.read_table(sql_file, from_table)
        except:
            traceback.print_exc()
            return "ERROR"

    else:
        return "UNSUPPORTED SQL. Available Kinds: PSQL (PostGreSQL), MSQL (MySQL), MDB (Access DB), SQLL (SQLite)"

    return df


def from_SQLQ(args):
    return "SQL Query Import coming right after sooon...."


df = get_dataframe_from_input(argss)

if not isinstance(df, type(pd.DataFrame())):
    print(df)
    sys.exit()

print(df.head(5))
print("5 Row Sample")
print(f"Full DataFrame Contains {len(df)} rows x {len(df.columns)} columns")

if argss.input_type.upper() == "XL" and len(df) == 1048576:
    print("EXCEL MAX ROWS: This may mean the exported data is too large for Excel format.")
    sys.exit()

push_to_psql(argss, df)

print("Done!")
