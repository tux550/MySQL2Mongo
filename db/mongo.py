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

"""
def rec_get_dependencies(table, forward_dependencies, visited, migration_rules, prefix=""):
    # If already visited, return inmediately
    if visited[table]:
        return list(forward_dependencies[table].keys())
    print(prefix+table)
    # If not visited, try to delete dependencies
    delete_keys = []
    for ref_table, cols_ls in forward_dependencies[table].items():
        dp_ls = rec_get_dependencies(ref_table, forward_dependencies, visited, migration_rules, prefix=prefix+">")
        print(dp_ls)
        if len(dp_ls) == 0:
            # TODO:fix
            #for col, ref_col in forward_dependencies[table][ref_table]:
            migration_rules[table][ref_table] = migration_rules[ref_table]
            del migration_rules[ref_table]
            #del forward_dependencies[table][ref_table]
            delete_keys.append(ref_table)
    for k in delete_keys:
        del forward_dependencies[table][k]
    visited[table] =True
    print(prefix+"!"+table)
    return list(forward_dependencies[table].keys())

def create_migration_plan(tables_metadata):
    forward_dependencies = get_dependencies(tables_metadata, forward=True)
    visited = {table:False for table in forward_dependencies}        
    migration_rules = {table:{} for table in forward_dependencies}
    print("1----")
    for table in forward_dependencies:
        if not visited[table]:
            rec_get_dependencies(table, forward_dependencies, visited, migration_rules)
    print("2----")
    #print(tables_metadata)
    print(forward_dependencies)
    print("MIGRATION RULES",migration_rules)

class RelationManyToMany():   
    def __init__(self, table_first, other_tables, table_aux) -> None:
        self.table_first  = table_first
        self.other_tables = other_tables
        self.table_aux   = table_aux
    def __repr__(self) -> str:
        return f"<M2M: {self.table_first}<->{self.other_tables} ({self.table_aux})>"
def create_migration_plan(tables_metadata, max_depth=1):
    forward_dependencies = get_dependencies(tables_metadata, forward=True)
    print(forward_dependencies)

    o2m_deps = []
    for table in forward_dependencies:
        o2m_deps.extend(
            [RelationOneToMany(table_one=table, table_many=ref_table) for ref_table in forward_dependencies[table]]
        )

    m2m_deps = []
    must_delete =[]
    for table in forward_dependencies:
        incoming = []
        outgoing = []
        for rel in o2m_deps:
            if table == rel.table_one:
                outgoing.append(rel)
            if table == rel.table_many:
                incoming.append(rel)
        if len(incoming) == 0 and len(outgoing) >= 2:
            first = outgoing[0].table_many
            others = [o.table_many for o in outgoing[1:]]
            m2m_deps.append(RelationManyToMany(table_first=first, other_tables=others, table_aux=table))
            must_delete.extend(outgoing)
    for rel in must_delete:
        o2m_deps.remove(rel)
    for rel in o2m_deps:
        print(rel)
    for rel in m2m_deps:
        print(rel)

    tables = {name: TablePlan(name) for name in forward_dependencies}
    migration_plan = {}
    for table in forward_dependencies:
        pass
    
"""

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

    def add_instruction(self, other_table):
        self.instructions[other_table] = True

    def __repr__(self) -> str:
        return f"<TablePlan {self.name} - {self.collection} - {self.instructions}>"

def create_migration_plan(tables_metadata, max_depth=1):
    forward_dependencies = get_dependencies(tables_metadata, forward=True)
    print(forward_dependencies)
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
                migration_plan[table].add_instruction( (o2m.col_one, o2m.table_many, o2m.col_many) )
    return migration_plan


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
