from msilib.text import tables
import pymongo
import datetime

def get_dependencies(tables_metadata, forward=True):
    tables_dependencies = {table_name : {} for table_name in tables_metadata}
    for table_name, metadata in tables_metadata.items():
        for col, val in metadata.items():
            if val:
                (ref_table, ref_col) = val
                if forward:
                    if ref_table not in tables_dependencies[table_name]:
                        tables_dependencies[table_name][ref_table] = []
                    tables_dependencies[table_name][ref_table].append( (col, ref_col) )
                else:
                    if table_name not in tables_dependencies[ref_table]:
                        tables_dependencies[ref_table][table_name] = []
                    tables_dependencies[ref_table][table_name].append( (col, ref_col) )

    return tables_dependencies

class RelationOneToMany():   
    def __init__(self, table_one, col_one, table_many, col_many) -> None:
        self.table_one  = table_one
        self.col_one    = col_one
        self.table_many = table_many
        self.col_many   = col_many
    def __repr__(self) -> str:
        return f"<O2M: {self.table_one} ({self.col_one}) ->{self.table_many} ({self.col_many})>"
    
class TablePlan():
    def __init__(self, name, collection=True, instructions=None) -> None:
        self.name       = name
        self.collection = collection
        if instructions is None:
            self.instructions = dict()
        else:
            self.instructions = instructions

    def add_instruction(self, other_table, local_key, other_key):
        self.instructions[other_table] = (local_key, other_key)

    def __repr__(self) -> str:
        return f"<TablePlan {self.name}>" #- {self.collection} 
def create_migration_plan(tables_metadata, max_depth=1):
    forward_dependencies = get_dependencies(tables_metadata, forward=True)
    o2m_deps = []
    for table in forward_dependencies:
        for ref_table, ref_cols_ls in forward_dependencies[table].items():
            for col, ref_col in ref_cols_ls:
                o2m_deps.append(
                    RelationOneToMany(table_one=table, table_many=ref_table, col_one=col, col_many=ref_col)
                )      
    o2m_many_tables = set([r.table_many for r in o2m_deps])
    migration_plan = {name: TablePlan(name) for name in forward_dependencies}
    for table in migration_plan:
        if table in o2m_many_tables:
            migration_plan[table].collection = False
        for o2m in o2m_deps:
            if o2m.table_one == table:
                migration_plan[table].add_instruction(other_table=migration_plan[o2m.table_many], local_key=o2m.col_one, other_key=o2m.col_many) 
    #return migration_plan
    
    final_plan = {}
    for table in migration_plan:
        if migration_plan[table].collection == True:
            final_plan[table] =migration_plan[table]

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
        print("LOADING:", table_plan)
        table_data = mysql_connector.get_table(table_plan.name)
        cleanup_collection(table_data)
        print(len(table_plan.instructions))
        for other_table_plan, (local_col, other_col) in table_plan.instructions.items():
            print(other_table_plan)
            other_table_data = self.load_table_plan(other_table_plan, mysql_connector)
            indexed = {row[other_col] : row for row in other_table_data}
            for item in table_data:
                reference = item[local_col]
                item[local_col] = indexed[reference]
                #item[local_col] = indexed[reference]
        return table_data
    def import_mysql(self,
                     mysql_connector,
                     delete_existing_documents=False):
        
        print("MYSQL IMPORT")
        #Iterate through the list of tables in the schema
        database_metadata = mysql_connector.get_tables_metadata()

        mongo_plan = create_migration_plan(database_metadata)
        for table_plan in mongo_plan.values():
            mongo_collection = self.database[table_plan.name]
            print(table_plan)
            #table_data = mysql_connector.get_table(table_plan.name)
            table_data = self.load_table_plan(table_plan, mysql_connector)
            if len(table_data) > 0:
                x = mongo_collection.insert_many(table_data)
                return len(x.inserted_ids)
            else:
                return 0

        print("MYSQL IMPORT END")

    def import_table(self,
                     table_name,
                     table_data,
                     delete_existing_documents=False):
        # Create mongo collection
        mongo_collection = self.database[table_name]
        # Delte collection
        if delete_existing_documents:
            mongo_collection.delete_many({})
        # Cleanup
        cleanup_collection(table_data)
        # Insert documents
        if len(table_data) > 0:
            x = mongo_collection.insert_many(table_data)
            return len(x.inserted_ids)
        else:
            return 0
