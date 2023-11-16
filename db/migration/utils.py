from .relation import RelationManyToMany, RelationOneToMany, TableAndCol

def get_o2m_relationships(
        tables_metadata : dict
    ) -> list[RelationOneToMany]:
    o2m_relationships = []
    for table_name, metadata in tables_metadata.items():
        for col, val in metadata.items():
            if val:
                (ref_table, ref_col) = val
                origin  = TableAndCol(table_name, col)
                destiny = TableAndCol(ref_table, ref_col)
                o2m_relationships.append(RelationOneToMany(destiny, origin))
    return o2m_relationships

def extract_m2m_relationships(
        tables_metadata : dict,
        o2m_deps : list[RelationOneToMany]
    ) -> list[RelationManyToMany]:
    m2m_deps   : list[RelationManyToMany] = []
    delete_o2m : list[RelationOneToMany] = []
    for table in tables_metadata:
        incoming : list[RelationOneToMany] = []
        outgoing : list[RelationOneToMany] = []
        for r in o2m_deps:
            if r.many.table == table:
                incoming.append(r)
            if r.one.table == table:
                outgoing.append(r)
        if len(outgoing) == 0 and len(incoming) >= 2:
            delete_o2m.extend(incoming)
            main   = incoming[0].many
            tables = [r.one for r in incoming]
            m2m_deps.append(RelationManyToMany(main=main, tables=tables))
    for r in delete_o2m:
        o2m_deps.remove(r)
    return m2m_deps

def get_relationships(
        tables_metadata : dict
    ) -> tuple[list[RelationOneToMany],list[RelationManyToMany]]:
    rels_o2m = get_o2m_relationships(tables_metadata=tables_metadata)
    rels_m2m = extract_m2m_relationships(tables_metadata=tables_metadata, o2m_deps=rels_o2m)
    return (rels_o2m, rels_m2m)