from parser_core.info_ddl_oop import DDLInfo
from snowflake.snowflake_conf import SNOWFLAKE_TYPE_MAP as typemap
from datetime import datetime
from common_trx.transformations import (
    build_reverse_typemap,
    generate_additional_dtype_cols,
    generate_additional_length_cols,
    generate_additional_upper_cols,
    get_elements,
    count_df_elements)
import os
import logging
from mainconfig import config

log_dir = config.log_dir
today = datetime.now().strftime("%Y%m%d")
logger = logging.getLogger("Snowflake_extension")
log_file = f"{log_dir}/{config.log_filename}_{today}.log"
logger.setLevel(logging.DEBUG)
os.makedirs(log_dir, exist_ok=True)




class SnowflakeExtend(DDLInfo):
    def __init__(self,ddl, source_db):
        super().__init__(ddl)
        logger.debug(f"|__init__| , estensione Snowflake avviata")
        logger.info(f"|__init__| , prevelo il dataframe dalla classe info_ddl_oop")
        self.dataframe_from_source = self.to_dataframe()
        if self.dataframe_from_source.empty:
            logger.error(f"|__init__| , dataframe non catturato da DDLinfo")
            raise ValueError(
                "DataFrame vuoto: impossibile inizializzare SnowflakeExtend"
            )
        logger.debug(f"|__init__| , dataframe catturato")
        self.config_conversion_dict = typemap
        reverse_typemap = build_reverse_typemap(typemap,source_system=source_db)
        logger.debug(f"|__init__| , typemap : {reverse_typemap}")
        self.conv_df = generate_additional_dtype_cols(self.dataframe_from_source,reverse_typemap)
        logger.debug(f"|__init__| , aggiunta lunghezza per Snowflake")
        self.dataframe_snw=generate_additional_length_cols(self.conv_df)
        if self.dataframe_snw.empty:
            raise ValueError(
                "DataFrame dataframe_snw: impossibile proseguire con le trasformazioni Snowflake"
            )
        self.dataframe_snw=generate_additional_upper_cols(self.dataframe_snw)
        #ONLY FOR SNOWFLAKE DETERMINISTIC FIELDS
        self.dataframe_snw.loc[
            self.dataframe_snw[config.add_field_dtype] == "TIMESTAMP_NTZ",
            config.add_field_length
        ] = 9
        self.dataframe_snw.loc[
            self.dataframe_snw[config.add_field_dtype] == "DATE",
            config.add_field_length
        ] = ''
        self.dataframe_snw.loc[
            (self.dataframe_snw[config.add_field_dtype] == "NUMBER") &
            (self.dataframe_snw["length"] == 0),
            config.add_field_length
        ] = '38,0'
        logger.debug(f"|__init__| , aggiunta lunghezza per Snowflake eseguita correttamente")
        self.snowfieldlist = get_elements(self.dataframe_snw,"column_name")
        self.snowfieldlist_upper = [
            x.upper() for x in get_elements(self.dataframe_snw, "column_name")
        ]
        self.snowfields = ",".join(self.snowfieldlist_upper)
        self.count_element_transformed = count_df_elements(self.dataframe_snw,self.snowfields)

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
    #df_snow=SnowflakeExtend(ddl, "sql_server").dataframe_snw
    print(SnowflakeExtend(ddl,"sql_server").snowfields)


