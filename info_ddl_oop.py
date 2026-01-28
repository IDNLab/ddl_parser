"""
Sviluppatore: Antonio Nunziante
Data_creazione 2026-01-06
"""

import re
import pandas as pd
import logging
import os
from datetime import datetime
import config

log_dir = config.log_dir
today = datetime.now().strftime("%Y%m%d")
logger = logging.getLogger("info_DDL_oop")
log_file = f"{log_dir}/{config.log_filename}_{today}.log"

os.makedirs(log_dir, exist_ok=True)


logger.setLevel(logging.DEBUG)


logging.basicConfig(
    filename=log_file,
    filemode="a",
    level=logging.DEBUG,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


class DDLInfo:
    def __init__(self, ddl: str, ddl_name = None):
        logger.debug(f"|__init__| , {ddl_name}")
        self.ddl = ddl
        logger.debug(f"|__init__| , {ddl_name} ,  ddl acquired: {self.ddl}")
        logger.debug(f"|__init__| , {ddl_name} ,  db_schema_table | acquiring")
        self.db_schema_table = self._get_db_schema_table()
        logger.debug(f"|__init__| , {ddl_name} ,  db_schema_table | acquired")
        logger.debug(f"|__init__| , {ddl_name} ,  columns_block | acquiring")
        self.columns_block = self._get_columns_block()
        logger.debug(f"|__init__| , {ddl_name} ,  columns_block | acquired")
        logger.debug(f"|__init__| , {ddl_name} ,  column_defs | acquiring")
        self.column_defs = self._split_column_defs(self.columns_block)
        logger.debug(f"|__init__| , {ddl_name} ,  column_defs | acquired")
        logger.debug(f"|__init__| , {ddl_name} ,  primary_keys | acquiring")
        self.primary_keys = self._get_primary_keys()
        logger.debug(f"|__init__| , {ddl_name} ,  primary_keys | acquired")
        logger.debug(f"|__init__| , {ddl_name} ,  foreign_keys | acquiring")
        self.foreign_keys = self._get_foreign_keys()
        logger.debug(f"|__init__| , {ddl_name} ,  foreign_keys | acquired")
        logger.debug(f"|__init__| , {ddl_name} ,  column_names | acquiring")
        self.column_names = [
            self._parse_column_name(d)
            for d in self.column_defs
            if self._parse_column_name(d)
        ]
        logger.debug(f"|__init__| , {ddl_name} ,  column_names | acquired")
        self.column_names_str = ",".join(self.column_names)
        #self.data_dict = self.to_dict()
        self.column_and_dt = ',\n'.join(
                                    f"{r['column_name']} {r['datatype']}"
                                    for r in self.to_dict()
                                    )
        self.count_element_ddl = len(self.to_dict())
    # -------------------------
    # METADATI TABELLA
    # -------------------------
    def _get_db_schema_table(self) -> dict:
        logger.info(f"|_get_db_schema_table| Parsing...")
        logger.debug(f"|_get_db_schema_table| ddl: \n {self.ddl}")
        pattern = re.compile(
            r"(?i)\bcreate\s+"
            r"(?:or\s+replace\s+)?"
            r"(?:temporary|temp|transient|dynamic)?\s*"
            r"table\s+"
            r"(?:"
            r"(?:"
            r"(?:`(?P<db_bt>[^`]+)`|\"(?P<db_dq>[^\"]+)\"|\[(?P<db_sq>[^\]]+)\]|(?P<db_u>\w+))"
            r"\s*\.\s*"
            r")?"
            r"(?:"
            r"(?:`(?P<sc_bt>[^`]+)`|\"(?P<sc_dq>[^\"]+)\"|\[(?P<sc_sq>[^\]]+)\]|(?P<sc_u>\w+))"
            r"\s*\.\s*"
            r")?"
            r"(?:"
            r"`(?P<tb_bt>[^`]+)`|\"(?P<tb_dq>[^\"]+)\"|\[(?P<tb_sq>[^\]]+)\]|(?P<tb_u>\w+)"
            r")"
            r")",
            re.IGNORECASE
        )

        match = pattern.search(self.ddl)
        logger.debug(f"|_get_db_schema_table| match: \n {match}")

        if not match:
            return {'database': None, 'schema': None, 'table': None}

        return {
            'database': match.group('db_bt') or match.group('db_dq') or match.group('db_sq') or match.group('db_u'),
            'schema': match.group('sc_bt') or match.group('sc_dq') or match.group('sc_sq') or match.group('sc_u'),
            'table': match.group('tb_bt') or match.group('tb_dq') or match.group('tb_sq') or match.group('tb_u')
        }

    def _get_columns_block(self) -> str:
        logger.debug(f"|_get_columns_block| START")
        match = re.search(r"\((.*)\)", self.ddl, re.DOTALL)
        logger.debug(f"|_get_columns_block| match: \n {match.group(1)}")
        logger.debug(f"|_get_columns_block| END")
        return match.group(1) if match else ""

    @staticmethod
    def _split_column_defs(block: str) -> list:
        logger.debug(f"|_split_column_defs| START")
        output = [p.strip() for p in re.split(r",(?![^()]*\))", block)]
        logger.debug(f"|_split_column_defs| {output}")
        logger.debug(f"|_split_column_defs| END")
        return output

    # -------------------------
    # CONSTRAINT
    # -------------------------
    def _get_primary_keys(self) -> list:
        logger.debug(f"|_get_primary_keys| START")
        pk_columns = []
        for d in self.column_defs:
            logger.debug(f"|_get_primary_keys| cycle, parsing primary key {d}")
            match = re.search(
                r"(?i)(?:constraint\s+\w+\s+)?primary\s+key\b[^(]*\(([^)]+)\)",
                d
            )
            logger.debug(f"|_get_primary_keys| cycle, matching primary key: {match}")
            if match:
                for col in match.group(1).split(','):
                    logger.debug(f"|_get_primary_keys| cycle, matching group of primary key: {col}")
                    pk_columns.append(col.strip().strip('`"'))
                    logger.debug(f"|_get_primary_keys| cycle, matching group of primary key found: {pk_columns}")
        logger.debug(f"|_get_primary_keys| PK = {pk_columns}")
        logger.debug(f"|_get_primary_keys| END")
        return pk_columns

    def _get_foreign_keys(self) -> list:
        logger.debug(f"|_get_foreign_keys| START")
        fk_constraints = []
        for d in self.column_defs:
            logger.debug(f"|_get_foreign_keys| cycle: {d}")
            match = re.search(
                r"(?i)(?:constraint\s+\w+\s+)?foreign\s+key\s*\(([^)]+)\)\s+references\s+([^\s(]+)\s*\(([^)]+)\)",
                d
            )
            if match:
                logger.debug(f"|_get_foreign_keys| cycle match: {match}")
                local_cols = [c.strip().strip('`"') for c in match.group(1).split(',')]
                logger.debug(f"|_get_foreign_keys| cycle match| local cols| : {local_cols}")
                ref_table = match.group(2).strip().strip('`"')
                logger.debug(f"|_get_foreign_keys| cycle match| local cols| ref_table: {ref_table}")
                ref_cols = [c.strip().strip('`"') for c in match.group(3).split(',')]
                logger.debug(f"|_get_foreign_keys| cycle match| local cols| ref_table | ref_cols : {ref_cols}")

                for l, r in zip(local_cols, ref_cols):
                    logger.debug(f"|_get_foreign_keys| cycle match| local cols| ref_table | ref_cols | cycle for ref_cols: {l}")
                    fk_constraints.append({
                        'column': l,
                        'ref_table': ref_table,
                        'ref_column': r
                    })
                    logger.debug(
                        f"|_get_foreign_keys| cycle match| local cols| ref_table | ref_cols | cycle for ref_cols| cycle for ref_cols|fk_constraint: {fk_constraints}")
        logger.debug(f"|_get_foreign_keys| END")
        return fk_constraints

    # -------------------------
    # PARSING COLONNE
    # -------------------------
    @staticmethod
    def _parse_column_name(definition: str):
        logger.debug(f"|_parse_column_name| CHECK MATCH FOR {definition}")
        if re.match(r"(?i)^(constraint|primary\s+key|foreign\s+key|unique|check)\b", definition):
            logger.debug(f"|_parse_column_name| CHECK MATCH FOR {definition} | IS NOT A MATCH"  )
            return None
        match = re.match(
            r"^(`(?P<n1>[^`]+)`|\"(?P<n2>[^\"]+)\"|(?P<n3>\w+))",
            definition
        )
        logger.debug(f"|_parse_column_name| CHECK MATCH FOR {definition} | IT IS A MATCH")
        output = match.group('n1') or match.group('n2') or match.group('n3') if match else None
        logger.debug(f"|_parse_column_name| CHECK MATCH FOR {definition} | IT IS A MATCH| result: {output}")
        logger.debug(f"|_parse_column_name| END CHECK FOR {definition}")
        return output

    @staticmethod
    def _parse_datatype(definition: str):
        logger.debug(f"|_parse_datatype| START FOR {definition}")
        match = re.match(
            r"^(`[^`]+`|\"[^\"]+\"|\w+)\s+(?P<type>[A-Za-z]+)",
            definition,
            re.IGNORECASE
        )
        output = match.group('type') or None
        logger.debug(f"|_parse_datatype| END FOR {definition}")
        return output

    @staticmethod
    def _parse_length(definition: str):
        logger.debug(f"|_parse_length| START FOR {definition}")
        match = re.match(
            r"^(`[^`]+`|\"[^\"]+\"|\w+)\s+[A-Za-z]+\(([^)]+)\)",
            definition,
            re.IGNORECASE
        )
        if not match:
            return 0
        numeric = re.match(r"^[\d,]+", match.group(2).strip())
        output =  numeric.group(0) if numeric else 0
        logger.debug(f"|_parse_length| END FOR {definition}")
        return output

    # -------------------------
    # OUTPUT
    # -------------------------
    def to_dict(self) -> list:
        logger.debug(f"|to_dict| START")
        meta = []
        db = self.db_schema_table.get('database') or ''
        sc = self.db_schema_table.get('schema') or ''
        tb = self.db_schema_table.get('table') or ''

        fq_table = '.'.join(p for p in [db, sc, tb] if p)

        for d in self.column_defs:
            name = self._parse_column_name(d)
            dtype = self._parse_datatype(d)
            if not name or not dtype:
                continue

            fk = next((f for f in self.foreign_keys if f['column'] == name), None)

            meta.append({
                'fully_qualified_table': fq_table,
                'database': db,
                'schema_name': sc,
                'table_name': tb,
                'column_name': name,
                'datatype': dtype,
                'length': self._parse_length(d),
                'is_key': 'Y' if name in self.primary_keys else 'N',
                'is_foreign': 'Y' if fk else 'N',
                'foreign_table': fk['ref_table'] if fk else None,
                'foreign_key': fk['ref_column'] if fk else None
            })
        logger.debug(f"|to_dict| END")
        return meta

    def to_dataframe(self) -> pd.DataFrame:
        logger.debug(f"|TO DATAFRAME| START")
        logger.debug(f"|TO DATAFRAME| Building dict to convert:")
        data = self.to_dict()
        if not data:
            logger.debug(f"|TO DATAFRAME| Building dict to convert: Not acquirable")
            return pd.DataFrame()
        logger.debug(f"|TO DATAFRAME| Building dict to convert: acquired. Building Dataframe")
        logger.debug(f"|TO DATAFRAME| END")
        return pd.DataFrame(data)[data[0].keys()]

if __name__ == '__main__':
    ddl = """CREATE OR REPLACE TRANSIENT TABLE dbo.RENT.Clienti (
    ClienteID INT(11) NOT NULL,
    CodiceFiscale CHAR(VARCHAR 16) NOT NULL,
    Nome NVARCHAR(100) NOT NULL,
    Cognome NVARCHAR(100) NOT NULL,
    DataNascita DATE NULL,
    Email NVARCHAR(255) NULL,
    Attivo BIT NOT NULL CONSTRAINT DF_Clienti_Attivo DEFAULT (1),
    DataCreazione DATETIME2(3) NOT NULL 
        CONSTRAINT DF_Clienti_DataCreazione DEFAULT (SYSDATETIME()),
    CONSTRAINT PK_Clienti PRIMARY KEY CLUSTERED (ClienteID),
    CONSTRAINT UQ_Clienti_CodiceFiscale FOREIGN KEY (CodiceFiscale),
    CONSTRAINT CK_Clienti_Email CHECK (Email LIKE '%@%.%')
);"""

    #print(DDLInfo(ddl).db_schema_table)
    #print(DDLInfo(ddl).column_defs)
    #print("Dataframe:")
    #print(DDLInfo(ddl).to_dataframe())
    #print("dict:")
    #print(DDLInfo(ddl).data_dict)
    #print("Number of parsed element : ",DDLInfo(ddl).count_element_ddl)
    #print("columns : ",DDLInfo(ddl).column_names_str)
    #print("Columns and datatype: \n"+DDLInfo(ddl).column_and_dt)
    DDLInfo(ddl).to_dataframe().to_html("ddl_parsed.html", index=False)
