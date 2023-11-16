import pymongo
import datetime
from msilib.text import tables
from .mysql import MySQLConnection
from .migration.relation import TableAndCol, RelationOneToMany, RelationManyToMany
from .migration.plan import TablePlan
from .migration.utils import get_relationships




def create_migration_plan(tables_metadata,database_nrows, max_size=None,max_depth=None):
    # Get table dependencies
    #print(tables_metadata)
    o2m_deps, m2m_deps = get_relationships(tables_metadata)
    #print("O2M deps", o2m_deps)
    #print("M2M deps", m2m_deps)
                
    o2m_origin_tables = set([r.one.table for r in o2m_deps])
    migration_plan = {name: TablePlan(name) for name in tables_metadata}
    
    # ADD O2M DEPENDENCIES TO PLAN
    for table in migration_plan:
        if table in o2m_origin_tables:
            migration_plan[table].collection = False
        for o2m in o2m_deps:
            if o2m.many.table == table:
                print("ADDED O2M", table, o2m.one.table)
                migration_plan[table].add_instruction(other_table=migration_plan[o2m.one.table], local_key=o2m.many.col, other_key=o2m.one.col, mode="single") 
    # ADD M2M DEPENDENCIES TO PLAN
    tmp = []
    for ls in [[t.table for t in r.tables] for r in m2m_deps]:
        tmp.extend(ls)
    m2m_origin_tables = set(tmp)
    m2m_main_tables = set([r.get_main_table().table for r in m2m_deps])
    for table in migration_plan:
        if table in m2m_origin_tables:
            migration_plan[table].collection = True
        if table in m2m_main_tables:
            migration_plan[table].collection = False
        for m2m in m2m_deps:
            # Choose collection
            selected = m2m.get_selected_table()
            if selected.table == table:
                print("ADDED M2M", table, selected.col, m2m.get_main_table().table, [t.table for t in m2m.get_other_tables()])
                migration_plan[table].add_instruction(other_table=migration_plan[m2m.get_main_table().table], local_key=selected.col, other_key=m2m.get_main_table().col, mode="multiple") 
    # ADD SIZE BASED RULES
    for table in migration_plan:
        if max_size is not None and database_nrows[table] > max_size:
            migration_plan[table].collection = True
            print("FORCED", table, database_nrows[table])
    # CREATE CLEAN PLAN
    final_plan = {}
    for table in migration_plan:
        if table not in m2m_main_tables:
            if migration_plan[table].collection == True:
                final_plan[table] =migration_plan[table]
    print("FINAL PLAN",final_plan)
    return final_plan


def cleanup_collection(collection : list[dict]) -> None:
    for document in collection:
        for attr in document:
            # Special case: Datetime
            if type(document[attr]) == datetime.date:
                document[attr] = datetime.datetime.combine(
                    document[attr], datetime.time.min
                )
        
class MongoConnection():
    def __init__(self, mongo_config) -> None:
        self.config   = mongo_config
        self.client   = pymongo.MongoClient(mongo_config["uri"])
        self.database = self.client[mongo_config["database"]]
            
    def database_exists(self) -> bool:
        dblist = self.client.list_database_names()
        return self.config["database"] in dblist

    def load_table_plan(self, table_plan, mysql_connector):
        print("LOADING TABLE PLAN:", table_plan.name)
        # Read from mysql
        table_data = mysql_connector.get_table(table_plan.name)
        cleanup_collection(table_data)
        # Recursive calls
        for other_table_plan, (local_col, other_col, mode) in table_plan.instructions.items():
            other_table_data = self.load_table_plan(other_table_plan, mysql_connector)
            # Join by index
            if mode == "single":
                indexed = {row[other_col] : row for row in other_table_data}
                for item in table_data:
                    reference = item[local_col]
                    item[other_table_plan.name] = indexed[reference]
                    del item[local_col]
            else:
                indexed = {}
                for row in other_table_data:
                    if row[other_col] not in indexed:
                        indexed[row[other_col]] =[]
                    indexed[row[other_col]].append(row)
                for item in table_data:
                    reference = item[local_col]
                    item[other_table_plan.name] = indexed[reference]
                    del item[local_col]
        return table_data
    
    def import_mysql(self,
                     mysql_connector : MySQLConnection,
                     delete_existing_documents=False,
                     max_rows=None):
        
        print("MYSQL IMPORT START")
        #Iterate through the list of tables in the schema
        database_metadata = mysql_connector.get_tables_metadata()
        database_nrows = mysql_connector.get_tables_nrows()
        print("DATABASE METADATA", database_metadata)
        print("DATABASE NROWS", database_nrows)
        mongo_plan = create_migration_plan(database_metadata,database_nrows)
        for table_plan in mongo_plan.values():
            mongo_collection = self.database[table_plan.name]
            # Delte collection
            if delete_existing_documents:
                mongo_collection.delete_many({})
            # Load table from plan recursievly
            #print("MONGO PLAN",mongo_plan)
            table_data = self.load_table_plan(table_plan, mysql_connector)
            # Save result
            if len(table_data) > 0:
                x = mongo_collection.insert_many(table_data)
                #return len(x.inserted_ids)
            #else:
            #    return 0
        print("MYSQL IMPORT END")

