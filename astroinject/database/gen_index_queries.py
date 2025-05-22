
def make_pg_sphere_index(table, ra_col, dec_col):
    ra_col = ra_col.lower()
    dec_col = dec_col.lower()
    
    if "." in table:
        wtdtable = table.split(".")[1]
    index_name = f"{wtdtable}_{ra_col}_{dec_col}_pgsphere_idx"
    
    query = f"""CREATE INDEX {index_name} 
    ON {table} USING gist (spoint(radians({ra_col}), radians({dec_col})));"""
  
    return query

def make_q3c_index(table, ra_col, dec_col):
    ra_col = ra_col.lower()
    dec_col = dec_col.lower()

    if "." in table:
        wtdtable = table.split(".")[1]
    else:
        wtdtable = table

    index_name = f"{wtdtable}_{ra_col}_{dec_col}_q3c_idx"

    query = f"""CREATE INDEX {index_name} 
    ON {table} (q3c_ang2ipix({ra_col}, {dec_col}));"""

    return query