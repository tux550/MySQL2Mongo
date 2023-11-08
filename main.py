# Author: tux550
# Base: https://github.com/alwaysanirudh/Migrate-MySQL-to-MongoDB/blob/master/migrate.py
# Python version: 3.10+

import pymongo
import datetime
from db import MySQLConnection, MongoConnection
from db.mongo import create_migration_plan
from utils.prettyprint import MsgType, prettyprint
import configparser

# Load config
CONFIG = configparser.ConfigParser()
CONFIG.read("config.ini")


begin_time = datetime.datetime.now()
prettyprint(f"Script started at: {begin_time}", MsgType.HEADER)

# ------------ OPTIONS ------------
delete_existing_documents = (CONFIG["Options"]["delete_existing_documents"]==True)

# ------------ MySQL connection ------------
prettyprint("Connecting to MySQL server...", MsgType.HEADER)
mysqldb = MySQLConnection(CONFIG["MySQL"])
print(  create_migration_plan(mysqldb.get_tables_metadata())  )
prettyprint("Connection to MySQL Server succeeded.", MsgType.OKGREEN)

# ------------ MongoDB connection ------------
prettyprint("Connecting to MongoDB server...", MsgType.HEADER)
mongodb = MongoConnection(CONFIG["Mongo"])
prettyprint("Connection to MongoDB Server succeeded.", MsgType.OKGREEN)

# ------------ Migration Start ------------
prettyprint("Migration started...", MsgType.HEADER)

# Validate database
if mongodb.database_exists():
    prettyprint("The database exists.", MsgType.OKBLUE)
else:
    prettyprint("The database does not exist, it is being created.", MsgType.WARNING)

#Iterate through the list of tables in the schema
tables = mysqldb.get_tables()

total_count = len(tables)
success_count = 0
fail_count = 0

for table in tables:
    try:
        prettyprint(f"Processing table: {table[0]}...", MsgType.OKCYAN)
        table_name = table[0]
        table_data = mysqldb.get_table(table_name)
        inserted_count = mongodb.import_table(table_name,table_data, delete_existing_documents)
        success_count += 1
        prettyprint(f"Processing table: {table_name} completed. {inserted_count} documents inserted.", MsgType.OKGREEN)
    except Exception as e:
        fail_count += 1
        prettyprint(f"{e}", MsgType.FAIL)

prettyprint("Migration completed.", MsgType.HEADER)
prettyprint(f"{success_count} of {total_count} tables migrated successfully.", MsgType.OKGREEN)
if fail_count > 0:
    prettyprint(f"Migration of {fail_count} tables failed. See errors above.", MsgType.FAIL)
    
end_time = datetime.datetime.now()
prettyprint(f"Script completed at: {end_time}", MsgType.HEADER)
prettyprint(f"Total execution time: {end_time-begin_time}", MsgType.HEADER)

