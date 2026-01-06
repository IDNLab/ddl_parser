"""
Sviluppatore: Antonio Nunziante
Data_creazione 2026-01-06
"""

import re
import pandas as pd


class DDLInfo:
    def __init__(self, ddl: str):
        self.ddl = ddl
        self.db_schema_table = self._get_db_schema_table()
        self.columns_block = self._get_columns_block()
        self.column_defs = self._split_column_defs(self.columns_block)
        self.primary_keys = self._get_primary_keys()
        self.foreign_keys = self._get_foreign_keys()
        self.column_names = [
            self._parse_column_name(d)
            for d in self.column_defs
            if self._parse_column_name(d)
        ]
    # -------------------------
    # METADATI TABELLA
    # -------------------------
    def _get_db_schema_table(self) -> dict:
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
        if not match:
            return {'database': None, 'schema': None, 'table': None}

        return {
            'database': match.group('db_bt') or match.group('db_dq') or match.group('db_sq') or match.group('db_u'),
            'schema': match.group('sc_bt') or match.group('sc_dq') or match.group('sc_sq') or match.group('sc_u'),
            'table': match.group('tb_bt') or match.group('tb_dq') or match.group('tb_sq') or match.group('tb_u')
        }

    def _get_columns_block(self) -> str:
        match = re.search(r"\((.*)\)", self.ddl, re.DOTALL)
        return match.group(1) if match else ""

    @staticmethod
    def _split_column_defs(block: str) -> list:
        return [p.strip() for p in re.split(r",(?![^()]*\))", block)]

    # -------------------------
    # CONSTRAINT
    # -------------------------
    def _get_primary_keys(self) -> list:
        pk_columns = []
        for d in self.column_defs:
            match = re.search(
                r"(?i)(?:constraint\s+\w+\s+)?primary\s+key\b[^(]*\(([^)]+)\)",
                d
            )
            if match:
                for col in match.group(1).split(','):
                    pk_columns.append(col.strip().strip('`"'))
        return pk_columns

    def _get_foreign_keys(self) -> list:
        fk_constraints = []
        for d in self.column_defs:
            match = re.search(
                r"(?i)(?:constraint\s+\w+\s+)?foreign\s+key\s*\(([^)]+)\)\s+references\s+([^\s(]+)\s*\(([^)]+)\)",
                d
            )
            if match:
                local_cols = [c.strip().strip('`"') for c in match.group(1).split(',')]
                ref_table = match.group(2).strip().strip('`"')
                ref_cols = [c.strip().strip('`"') for c in match.group(3).split(',')]

                for l, r in zip(local_cols, ref_cols):
                    fk_constraints.append({
                        'column': l,
                        'ref_table': ref_table,
                        'ref_column': r
                    })
        return fk_constraints

    # -------------------------
    # PARSING COLONNE
    # -------------------------
    @staticmethod
    def _parse_column_name(definition: str):
        if re.match(r"(?i)^(constraint|primary\s+key|foreign\s+key|unique|check)\b", definition):
            return None
        match = re.match(
            r"^(`(?P<n1>[^`]+)`|\"(?P<n2>[^\"]+)\"|(?P<n3>\w+))",
            definition
        )
        return match.group('n1') or match.group('n2') or match.group('n3') if match else None

    @staticmethod
    def _parse_datatype(definition: str):
        match = re.match(
            r"^(`[^`]+`|\"[^\"]+\"|\w+)\s+(?P<type>[A-Za-z]+)",
            definition,
            re.IGNORECASE
        )
        return match.group('type') if match else None

    @staticmethod
    def _parse_length(definition: str):
        match = re.match(
            r"^(`[^`]+`|\"[^\"]+\"|\w+)\s+[A-Za-z]+\(([^)]+)\)",
            definition,
            re.IGNORECASE
        )
        if not match:
            return 0
        numeric = re.match(r"^[\d,]+", match.group(2).strip())
        return numeric.group(0) if numeric else 0

    # -------------------------
    # OUTPUT
    # -------------------------
    def to_dict(self) -> list:
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
        return meta

    def to_dataframe(self) -> pd.DataFrame:
        data = self.to_dict()
        if not data:
            return pd.DataFrame()
        return pd.DataFrame(data)[data[0].keys()]

if __name__ == '__main__':
    ddl = """CREATE OR REPLACE TRANSIENT TABLE dbo.RENT.Clienti (
    ClienteID INT(11) NOT NULL,
    CodiceFiscale CHAR(16) NOT NULL,
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

    print(DDLInfo(ddl).db_schema_table)
    print(DDLInfo(ddl).column_defs)
    print(DDLInfo(ddl).column_names)
    print(DDLInfo(ddl).to_dataframe())
    DDLInfo(ddl).to_dataframe().to_html("ddl_parsed.html", index=False)
