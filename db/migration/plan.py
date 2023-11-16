class TablePlan():
    def __init__(self, name : str, collection:bool =True, instructions:dict|None=None) -> None:
        self.name       = name
        self.collection = collection
        if instructions is None:
            self.instructions = dict()
        else:
            self.instructions = instructions

    def add_instruction(self, other_table, local_key, other_key, mode):
        self.instructions[other_table] = (local_key, other_key, mode)

    def __repr__(self) -> str:
        return f"<TablePlan {self.name}>" #- {self.collection} 