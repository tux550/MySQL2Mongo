import mysql.connector


def mysql_connect(mysql_config):
    return mysql.connector.connect(
        host=mysql_config["host"],
        database=mysql_config["database"],
        user=mysql_config["user"],
        password=mysql_config["password"]
    )

class MySQLConnection():
    def __init__(self, mysql_config) -> None:
        self.schema     = mysql_config["schema"]
        self.connection = mysql_connect(mysql_config)

    def get_table_metadata(self, table_name, columns=None, fks = None):
        metadata = {}
        if columns is None:
            columns = self.get_columns()
        for table, col in columns:
            if table == table_name:
                metadata[col] = None
        if fks is None:
            fks = self.get_foreign_keys()
        for constraint_name, table, col, ref_table, ref_col in fks:
            if table == table_name:
                if ref_table and ref_col:
                    metadata[col] = (ref_table, ref_col) 
        return metadata
    
    def get_tables_metadata(self):
        tables_metadata = {}
        columns = self.get_columns()
        fks = self.get_foreign_keys()
        for table, in self.get_tables():
            tables_metadata[table] = self.get_table_metadata(table, columns, fks)
        return tables_metadata
    
    def get_tables_nrows(self) -> dict:
        table_list_cursor = self.connection.cursor()
        table_list_cursor.execute(
            "SELECT table_name,TABLE_ROWS FROM information_schema.tables WHERE table_schema = %s ORDER BY table_name;", (self.schema,)
        ) #DATA_LENGTH
        tables = table_list_cursor.fetchall()
        tables_sizes = {}
        for table, size in tables:
            tables_sizes[table] = size
        return tables_sizes

    
    def get_columns(self) -> list:
        table_list_cursor = self.connection.cursor()
        table_list_cursor.execute(
            "SELECT TABLE_NAME , COLUMN_NAME  FROM information_schema.columns WHERE table_schema = %s ORDER BY table_name;", (self.schema,)
        )
        tables = table_list_cursor.fetchall()
        return tables
    
    def get_tables(self) -> list:
        table_list_cursor = self.connection.cursor()
        table_list_cursor.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = %s ORDER BY table_name;", (self.schema,)
        )
        tables = table_list_cursor.fetchall()
        return tables


    def get_foreign_keys(self) -> list:
        table_list_cursor = self.connection.cursor()
        table_list_cursor.execute(
            "SELECT CONSTRAINT_NAME, TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME  FROM information_schema.KEY_COLUMN_USAGE  WHERE CONSTRAINT_SCHEMA = %s ORDER BY table_name;", (self.schema,)
        )
        tables = table_list_cursor.fetchall()
        return tables
    
    def get_constraints(self) -> list:
        table_list_cursor = self.connection.cursor()
        table_list_cursor.execute(
            "SELECT CONSTRAINT_NAME  FROM information_schema.TABLE_CONSTRAINTS WHERE CONSTRAINT_SCHEMA = %s ORDER BY table_name;", (self.schema,)
        )
        tables = table_list_cursor.fetchall()
        return tables
    
    def get_table(self, table_name) -> list:
        cursor = self.connection.cursor(dictionary=True)
        #print(table_name)
        cursor.execute("SELECT * FROM " + table_name + ";")
        result = cursor.fetchall() 
        return result



