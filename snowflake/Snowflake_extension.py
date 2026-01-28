from parser_core.info_ddl_oop import DDLInfo
from snowflake.snowflake_datatype_map import SNOWFLAKE_TYPE_MAP as typemap
from datetime import datetime
import os
import logging
from mainconfig import config

log_dir = config.log_dir
today = datetime.now().strftime("%Y%m%d")
logger = logging.getLogger("Snowflake_extension")
log_file = f"{log_dir}/{config.log_filename}_{today}.log"

os.makedirs(log_dir, exist_ok=True)


logger.setLevel(logging.DEBUG)


class SnowflakeExtend(DDLInfo):
    def __init__(self,ddl, source_db):
        super().__init__(ddl)
        self.dataframe_from_source = self.to_dataframe()
        self.config_conversion_dict = typemap
        reverse_typemap = build_reverse_typemap(
            typemap,
            source_system=source_db
        )
        if not self.dataframe_from_source.empty:
            self.dataframe_from_source["ADDITIONAL_DTYPE"] = (
                self.dataframe_from_source["datatype"]
                .str.upper()
                .map(reverse_typemap)
            )
        else:
            pass
        self.dataframe_snw = self.dataframe_from_source
        self.dataframe_snw["ADDITIONAL_LENGHT"] = (
            self.dataframe_snw["length"]
            .where(self.dataframe_snw["length"].notna() & (self.dataframe_snw["length"] != 0))
        )
        self.dataframe_snw.loc[
            self.dataframe_snw["ADDITIONAL_DTYPE"] == "TIMESTAMP_NTZ",
            "ADDITIONAL_LENGHT"
        ] = 9
        self.dataframe_snw.loc[
            self.dataframe_snw["ADDITIONAL_DTYPE"] == "DATE",
            "ADDITIONAL_LENGHT"
        ] = ''
        self.dataframe_snw.loc[
            (self.dataframe_snw["ADDITIONAL_DTYPE"] == "NUMBER") &
            (self.dataframe_snw["length"] == 0),
            "ADDITIONAL_LENGHT"
        ] = '38,0'



def build_reverse_typemap(typemap: dict, source_system: str) -> dict:
    reverse = {}

    for target_type, systems in typemap.items():
        if source_system not in systems:
            continue

        for src_type in systems[source_system]:
            reverse[src_type.upper()] = target_type

    return reverse

if __name__ == "__main__":
    ddl = """CREATE OR REPLACE TRANSIENT TABLE dbo.RENT.Clienti (
    ClienteID INT(11) NOT NULL,
    PROVA FLOAT(15,3),
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
    SnowflakeExtend(ddl,"sql_server").dataframe_snw.to_html("ddl_parsed_snow.html", index=False)


