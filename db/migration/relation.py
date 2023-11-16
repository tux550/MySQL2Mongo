from config import CONFIG
ask_m2m_select = CONFIG["Options"]["ask_m2m_select"] == "True"

class TableAndCol():
    def __init__(self, table : str, col : str) -> None:
        self.table = table
        self.col   = col 
    def __str__(self) -> str:
        return f"{self.table}.{self.col}"
    def __repr__(self) -> str:
        return f"<TC: {self.table} ({self.col})>"

class RelationOneToMany():   
    def __init__(self, one : TableAndCol, many :TableAndCol) -> None:
        self.one  = one
        self.many = many
    def __repr__(self) -> str:
        return f"<O2M: {str(self.one)} --> {str(self.many)} >"

class RelationManyToMany():   
    def __init__(self, mains : list[TableAndCol], tables : list[TableAndCol]) -> None:
        self.mains   = mains
        self.tables  = tables
        self.selected_table_index = self.determine_selected()
        
    def determine_selected(self):
        print("HERE")
        if ask_m2m_select:
            print(f"SELECT TABLE FOR M2M RELATIONSHIP: {self.mains[0].table}")
            for idx, table in enumerate(self.tables):
                print(f"{idx}: {table.table}.{table.col} -> {self.mains[idx].table}")         
            return int(input())
        else:
            return 0

    def get_main_table(self):
        return self.mains[self.selected_table_index]
    
    def get_selected_table(self):
        return self.tables[self.selected_table_index]

    def get_other_tables(self):
        return [self.tables[i] for i in range(len(self.tables)) if i != self.selected_table_index]
        
    def __repr__(self) -> str:
        return f"<M2M: {str(self.get_main_table())} FOR {str(self.tables)} >"

"""
class RelationOneToMany():   
    def __init__(self, table_one, col_one, table_many, col_many) -> None:
        self.table_one  = table_one
        self.col_one    = col_one
        self.table_many = table_many
        self.col_many   = col_many
    def __repr__(self) -> str:
        return f"<O2M: {self.table_one} ({self.col_one}) ->{self.table_many} ({self.col_many})>"

class RelationManyToMany():   
    def __init__(self, table_first, col_first, table_aux, col_aux) -> None:
        self.table_first  = table_first
        self.col_first    = col_first
        self.table_aux    = table_aux
        self.col_aux      = col_aux
    def __repr__(self) -> str:
        return f"<O2M: {self.table_first} ({self.col_first}) ->{self.table_aux} ({self.col_aux})>"
"""