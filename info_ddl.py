import re  # Importa il modulo delle espressioni regolari


# Estrae database, schema e nome tabella da una DDL CREATE TABLE

def get_db_schema_table(ddl: str) -> dict:
    pattern = re.compile(
        r"(?i)create\s+(?:or\s+replace\s+)?table\s+"  # Match iniziale CREATE TABLE con opzione OR REPLACE
        r"(?:`(?P<database>[^`]+)`\.|(?P<database2>\w+)\.)?"  # Cattura nome database se presente
        r"(?:`(?P<schema>[^`]+)`\.|(?P<schema2>\w+)\.)?"  # Cattura nome schema se presente
        r"`?(?P<table>[^`\s(]+)`?",  # Cattura nome tabella
        re.IGNORECASE
    )
    match = pattern.search(ddl)  # Cerca nel testo la corrispondenza
    if not match:
        return {'database': None, 'schema': None, 'table': None}  # Nessuna corrispondenza trovata
    return {
        'database': match.group('database') or match.group('database2'),  # Estrae database
        'schema': match.group('schema') or match.group('schema2'),  # Estrae schema
        'table': match.group('table')  # Estrae tabella
    }


# Estrae il contenuto tra parentesi della definizione di tabella (colonne)
def get_columns_block(ddl: str) -> str:
    match = re.search(r"\((.*)\)", ddl, re.DOTALL)  # Trova tutto ciò che è tra la prima parentesi aperta e l'ultima chiusa
    return match.group(1) if match else ""  # Ritorna il contenuto tra parentesi

def get_primary_keys(ddl: str) :
    """
    Estrae i nomi delle colonne che fanno parte della PRIMARY KEY
    da una DDL SQL, anche se definite con CONSTRAINT.
    """
    block = get_columns_block(ddl)
    defs = split_column_defs(block)
    pk_columns = []

    for d in defs:
        # Cerca PRIMARY KEY anche se preceduta da CONSTRAINT opzionale
        match = re.search(r"(?i)(?:constraint\s+\w+\s+)?primary\s+key\s*\(([^)]+)\)", d.strip())
        if match:
            columns = match.group(1)
            for col in columns.split(','):
                col_name = col.strip().strip('`"')
                pk_columns.append(col_name)

    return pk_columns

def get_foreign_keys(ddl: str) :
    """
    Estrae le FOREIGN KEY da una DDL SQL.
    Restituisce una lista di dizionari con:
    - column: nome colonna locale
    - ref_table: tabella referenziata
    - ref_column: colonna referenziata
    """
    block = get_columns_block(ddl)
    defs = split_column_defs(block)
    fk_constraints = []

    for d in defs:
        # Cerca FOREIGN KEY con tabella e colonna referenziata
        match = re.search(
            r"(?i)(?:constraint\s+\w+\s+)?foreign\s+key\s*\(([^)]+)\)\s+references\s+([^\s(]+)\s*\(([^)]+)\)",
            d.strip()
        )
        if match:
            local_cols = [c.strip().strip('`"') for c in match.group(1).split(',')]
            ref_table = match.group(2).strip().strip('`"')
            ref_cols = [c.strip().strip('`"') for c in match.group(3).split(',')]

            # Associazione 1:1 tra colonne locali e referenziate
            for local_col, ref_col in zip(local_cols, ref_cols):
                fk_constraints.append({
                    'column': local_col,
                    'ref_table': ref_table,
                    'ref_column': ref_col
                })

    return fk_constraints


# Divide il blocco colonne in singole definizioni, evitando di spezzare su virgole interne

def split_column_defs(block: str) -> list:
    return [part.strip() for part in re.split(r",(?![^()]*\))", block)]  # Split su virgole non annidate


# Estrae il nome della colonna, ignorando vincoli come PRIMARY KEY, CHECK, ecc.
def parse_column_name(definition: str) :
    if re.match(r"(?i)^(constraint|primary\s+key|foreign\s+key|unique|check)\b", definition):
        return None  # Ignora righe che definiscono vincoli
    match = re.match(r"^(`(?P<name1>[^`]+)`|\"(?P<name2>[^\"]+)\"|(?P<name3>\w+))", definition)  # Cattura il nome della colonna
    if match:
        return match.group('name1') or match.group('name2') or match.group('name3')  # Restituisce il nome trovato
    return None


# Estrae il tipo di dato della colonna (es: INT, VARCHAR, DECIMAL)
def parse_datatype(definition: str) :
    match = re.match(
        r"^(`[^`]+`|\"[^\"]+\"|\w+)\s+(?P<type>[A-Za-z]+)(?:\([^)]*\))?",  # Match sul tipo dopo il nome
        definition,
        re.IGNORECASE
    )
    return match.group('type') if match else None  # Ritorna il tipo trovato


# Estrae la lunghezza del tipo di dato, se presente (es: VARCHAR(255), DECIMAL(10,2))
def parse_length(definition: str) :
    """
    Estrae il contenuto tra parentesi dopo il tipo di dato, rimuovendo eventuali descrittori testuali.
    Esempi:
    - VARCHAR(255 char) -> '255'
    - DECIMAL(10,2)     -> '10,2'
    """
    match = re.match(
        r"^(`[^`]+`|\"[^\"]+\"|\w+)\s+[A-Za-z]+\((?P<length>[^)]+)\)",  # Match su valore tra parentesi
        definition,
        re.IGNORECASE
    )
    if match:
        raw_length = match.group('length')  # Contenuto grezzo tra parentesi
        numeric_part = re.match(r"^[\d,]+", raw_length.strip())  # Solo numeri o virgola
        return numeric_part.group(0) if numeric_part else None  # Ritorna la parte numerica
    return None


# Coordina il parsing per estrarre nome, tipo e lunghezza delle colonne dalla DDL

def get_columns_info(ddl: str) :
    block = get_columns_block(ddl)  # Estrae il blocco colonne
    defs = split_column_defs(block)  # Divide in definizioni singole
    pk = get_primary_keys(ddl)  # Lista dei nomi delle colonne PK
    fk = get_foreign_keys(ddl)  # Lista di dict con info FK
    columns = []
    for d in defs:
        name = parse_column_name(d)  # Estrae nome colonna
        dtype = parse_datatype(d)  # Estrae tipo colonna
        length = parse_length(d)  # Estrae lunghezza colonna, se presente
        if name and dtype:
            info = {'name': name, 'type': dtype}  # Dizionario colonna base
            if length is not None:
                info['length'] = length  # Aggiunge lunghezza se trovata

            # Chiave primaria
            info['is_key'] = 'Y' if name in pk else 'N'

            # Chiave esterna (se presente), altrimenti None
            fk_entry = next((entry for entry in fk if entry['column'] == name), None)
            if fk_entry:
                info['is_foreign'] = 'Y'
                info['foreign_table'] = fk_entry['ref_table']
                info['foreign_key'] = fk_entry['ref_column']
            else:
                info['is_foreign'] = 'N'
                info['foreign_table'] = None
                info['foreign_key'] = None

            columns.append(info)  # Aggiunge la colonna all'elenco
    return columns


# Esempio di utilizzo
if __name__ == '__main__':
    ddl_examples = [
        """CREATE TABLE warehouse.staging.users (
    user_id INT NOT NULL,
    name VARCHAR(255),
    role_id INT,
    CONSTRAINT pk_users PRIMARY KEY(user_id),
    CONSTRAINT fk_role FOREIGN KEY(role_id) REFERENCES warehouse.staging.roles(role_id)
);""",
        "CREATE OR REPLACE TABLE `warehouse`.`staging`.`users` ( `user_id` INT, `name` VARCHAR(255 char), details JSON, CONSTRAINT PRIMARY KEY(user_id),CONSTRAINT PRIMARY KEY(name) )",
        "CREATE TABLE metrics (\"total_count\" INT, value FLOAT)"
    ]
    for ddl in ddl_examples:
        print(f"DDL: {ddl}")  # Stampa la DDL originale
        print("Meta:", get_db_schema_table(ddl))  # Stampa database, schema e tabella
        print("Columns Info:", get_columns_info(ddl))  # Stampa info colonne
        print()
