"""
Sviluppatore: Antonio Nunziante
"""

import re  # Importa il modulo delle espressioni regolari
import pandas as pd


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

def get_columns_info(ddl: str,out = 'dataframe') :
    full_meta = get_db_schema_table(ddl) #estraggo database schema e tabella se ci sono
    table=full_meta.get('table') or ''
    schema=full_meta.get('schema') or ''
    database = full_meta.get('database') or ''
    print(database)
    parts = []
    if database:
        parts.append(database)
    if schema:
        parts.append(schema)
    parts.append(table)  # assumendo che table sia sempre presente
    fq_table = '.'.join(parts) #unisco i metadati della tabella in una sola variabile
    print(fq_table)
    block = get_columns_block(ddl)  # Estrae il blocco colonne
    defs = split_column_defs(block)  # Divide in definizioni singole
    pk = get_primary_keys(ddl)  # Lista dei nomi delle colonne PK
    fk = get_foreign_keys(ddl)  # Lista di dict con info FK
    metadata = []
    for d in defs:
        name = parse_column_name(d)  # Estrae nome colonna
        dtype = parse_datatype(d)  # Estrae tipo colonna
        length = parse_length(d)  # Estrae lunghezza colonna, se presente
        if name and dtype:
            info = {'fully_qualified_table':fq_table,'column_name': name, 'datatype': dtype}  # Dizionario colonna base
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
            metadata.append(info)  # Aggiunge la colonna all'elenco
    if out=='dict':
        return metadata
    elif out =='dataframe':
        if not metadata:
            return pd.DataFrame()
        df=pd.DataFrame(metadata)
        return df[metadata[0].keys()]
    else:
        raise ValueError("Parametro out non valido.Usare dict o dataframe")


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
        "CREATE TABLE metrics (\"total_count\" INT, value FLOAT)",
        """CREATE OR REPLACE TABLE "MY_DATABASE"."MY_SCHEMA"."MY_COMPLEX_TABLE" (
    ID NUMBER(38,0) NOT NULL COMMENT 'Primary key',
    USER_ID UUID NOT NULL,
    USER_NAME STRING NOT NULL,
    EMAIL VARCHAR(255),
    IS_ACTIVE BOOLEAN DEFAULT TRUE,
    CREATED_AT TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP(),
    METADATA VARIANT,
    TAGS ARRAY,
    ATTRIBUTES OBJECT,
    SCORE FLOAT,
    BALANCE DECIMAL(10,2),
    STATUS STRING CHECK (STATUS IN ('ACTIVE', 'INACTIVE', 'SUSPENDED')),
    UPDATED_AT TIMESTAMP_NTZ,
    CONSTRAINT PK_ID PRIMARY KEY (ID),
    CONSTRAINT CHK_SCORE CHECK (SCORE >= 0),
    CONSTRAINT UQ_EMAIL UNIQUE (EMAIL),
    CONSTRAINT FK_USER FOREIGN KEY (USER_ID) REFERENCES MY_DATABASE.MY_SCHEMA.USERS(ID)
)
COMMENT = 'A very complex table with many data types and constraints'
DATA_RETENTION_TIME_IN_DAYS = 7
CHANGE_TRACKING = TRUE
CLUSTER BY (USER_ID, STATUS);
""",
        """-- Creazione di uno schema complesso con DDL Oracle

-- Tabella principale con partizionamento, LOB, e vincoli avanzati
CREATE TABLE "HR"."MEGA_TABLE" (
    "ID" NUMBER(20) GENERATED ALWAYS AS IDENTITY 
        (START WITH 1000 INCREMENT BY 2 CACHE 100) CONSTRAINT PK_MEGA PRIMARY KEY,
    "UUID" RAW(16) DEFAULT SYS_GUID(),
    "GEOMETRY" SDO_GEOMETRY,
    "JSON_DATA" CLOB CHECK ("JSON_DATA" IS JSON),
    "COMPRESSED_DATA" BLOB,
    "AUDIT_TIMESTAMP" TIMESTAMP(6) DEFAULT SYSTIMESTAMP,
    "STATUS" VARCHAR2(20) 
        CHECK ("STATUS" IN ('ACTIVE','SUSPENDED','DELETED')) 
        DEFAULT 'ACTIVE',
    "VERSION" NUMBER(8) DEFAULT 1,
    "PARENT_ID" NUMBER(20),
    "METADATA" XMLTYPE,
    "HISTORY" VARCHAR2(4000) 
        GENERATED ALWAYS AS (JSON_VALUE("JSON_DATA", '$.history')) VIRTUAL,
    
    CONSTRAINT "FK_PARENT" FOREIGN KEY ("PARENT_ID") 
        REFERENCES "HR"."PARENT_TABLE" ("ID") 
        ON DELETE CASCADE
        DEFERRABLE INITIALLY DEFERRED
)
LOB ("JSON_DATA") STORE AS SECUREFILE (
    TABLESPACE "LOB_DATA" 
    COMPRESS HIGH 
    DEDUPLICATE
)
PARTITION BY RANGE ("AUDIT_TIMESTAMP") 
INTERVAL (NUMTOYMINTERVAL(1, 'MONTH'))
SUBPARTITION BY HASH ("ID") SUBPARTITIONS 4
(
    PARTITION "P_INITIAL" VALUES LESS THAN (TO_DATE('2024-01-01', 'YYYY-MM-DD'))
)
COMPRESS FOR OLTP
PCTFREE 10
PCTUSED 40
INITRANS 4
PARALLEL 8
NOLOGGING;

-- Tabella di partizionamento esterno
CREATE TABLE "HR"."EXTERNAL_DATA"
(
    "TRANSACTION_ID" NUMBER,
    "AMOUNT" NUMBER(18,4),
    "CURRENCY" CHAR(3),
    "LOAD_DATE" DATE
)
ORGANIZATION EXTERNAL
(
    TYPE ORACLE_LOADER
    DEFAULT DIRECTORY "DATA_DIR"
    ACCESS PARAMETERS
    (
        RECORDS DELIMITED BY NEWLINE
        BADFILE 'bad_%a_%p.bad'
        LOGFILE 'log_%a_%p.log'
        FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"'
        MISSING FIELD VALUES ARE NULL
        (
            "TRANSACTION_ID",
            "AMOUNT",
            "CURRENCY",
            "LOAD_DATE" DATE "YYYY-MM-DD"
        )
    )
    LOCATION ('data_*.csv')
)
REJECT LIMIT UNLIMITED;

-- Tabella temporale con versioning
CREATE TABLE "HR"."TIME_TRAVEL" (
    "ID" NUMBER PRIMARY KEY,
    "DATA" VARCHAR2(100),
    "VALID_FROM" TIMESTAMP(6) WITH TIME ZONE,
    "VALID_TO" TIMESTAMP(6) WITH TIME ZONE
) 
FLASHBACK ARCHIVE "HISTORY_ARCHIVE";

-- Tabella nested con oggetti
CREATE TYPE "HR"."ADDRESS_TYP" AS OBJECT (
    "STREET" VARCHAR2(100),
    "CITY" VARCHAR2(50),
    "POSTAL_CODE" VARCHAR2(20)
);

CREATE TABLE "HR"."EMPLOYEES" (
    "EMP_ID" NUMBER(6) PRIMARY KEY,
    "NAME" VARCHAR2(100),
    "ADDRESS" "HR"."ADDRESS_TYP",
    "SALARY" NUMBER(8,2) INVISIBLE,
    "DEPARTMENT_ID" NUMBER(4)
NESTED TABLE "PROJECTS" STORE AS "PROJECTS_NT";

-- Indice bitmap join
CREATE BITMAP INDEX "HR"."SALARY_BMI" ON "HR"."EMPLOYEES" ("SALARY")
FROM "HR"."EMPLOYEES" e, "HR"."DEPARTMENTS" d
WHERE e."DEPARTMENT_ID" = d."DEPARTMENT_ID"
LOCAL;

-- Indice function-based con partizionamento
CREATE INDEX "HR"."IDX_UPPER_NAME" ON "HR"."EMPLOYEES" (UPPER("NAME"))
GLOBAL PARTITION BY HASH ("EMP_ID") 
PARTITIONS 8
PARALLEL 4;

-- Viste materializzate complesse
CREATE MATERIALIZED VIEW "HR"."SUMMARY_MV"
BUILD IMMEDIATE
REFRESH COMPLETE ON DEMAND
ENABLE QUERY REWRITE
AS 
SELECT d."DEPARTMENT_NAME", COUNT(e."EMP_ID"), AVG(e."SALARY")
FROM "HR"."EMPLOYEES" e
JOIN "HR"."DEPARTMENTS" d ON e."DEPARTMENT_ID" = d."DEPARTMENT_ID"
GROUP BY d."DEPARTMENT_NAME";

-- Politica di sicurezza
BEGIN
    DBMS_RLS.ADD_POLICY(
        object_schema => 'HR',
        object_name => 'EMPLOYEES',
        policy_name => 'SALARY_SECURITY',
        function_schema => 'SEC',
        policy_function => 'HIDE_SALARIES',
        statement_types => 'SELECT'
    );
END;
/

-- Commenti e metadata
COMMENT ON TABLE "HR"."MEGA_TABLE" IS 'Tabella principale per stress test DDL';
COMMENT ON COLUMN "HR"."MEGA_TABLE"."JSON_DATA" IS 'Documento JSON con dati complessi';"""
    ]
    for ddl in ddl_examples:
        print("Columns Info:", get_columns_info(ddl))  # Stampa info colonne
        print()
