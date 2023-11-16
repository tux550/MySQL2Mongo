# Author: tux550
# Base: https://github.com/alwaysanirudh/Migrate-MySQL-to-MongoDB/blob/master/migrate.py
# Python version: 3.10+

import pymongo
import datetime
from db import MySQLConnection, MongoConnection
from db.mongo import create_migration_plan
from utils.prettyprint import MsgType, prettyprint


# Load config
from config import CONFIG


# ------------ OPTIONS ------------
delete_existing_documents = (CONFIG["Options"]["delete_existing_documents"]==True)
max_rows = None if CONFIG["Options"]["max_rows"]=="None" else int(CONFIG["Options"]["max_rows"])

# ------------ MySQL connection ------------
prettyprint("Connecting to MySQL server...", MsgType.HEADER)
mysqldb = MySQLConnection(CONFIG["MySQL"])
prettyprint("Connection to MySQL Server succeeded.", MsgType.OKGREEN)

# ------------ MongoDB connection ------------
prettyprint("Connecting to MongoDB server...", MsgType.HEADER)
mongodb = MongoConnection(CONFIG["Mongo"])
#print( create_migration_plan(mysqldb.get_tables_metadata()))
prettyprint("Connection to MongoDB Server succeeded.", MsgType.OKGREEN)

# ------------ Migration Start ------------
prettyprint("Migration started...", MsgType.HEADER)
# Validate database
if mongodb.database_exists():
    prettyprint("The database exists.", MsgType.OKBLUE)
else:
    prettyprint("The database does not exist, it is being created.", MsgType.WARNING)

begin_time = datetime.datetime.now()
prettyprint(f"Migration started at: {begin_time}", MsgType.HEADER)
mongodb.import_mysql(mysqldb, delete_existing_documents, max_rows)
end_time = datetime.datetime.now()
prettyprint(f"Script completed at: {end_time}", MsgType.HEADER)
prettyprint(f"Total execution time: {end_time-begin_time}", MsgType.HEADER)
