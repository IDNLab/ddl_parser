SNOWFLAKE_TYPE_MAP = {
    "TIMESTAMP_NTZ": {
        "oracle": ["TIMESTAMP"],
        "sql_server": ["DATETIME","DATETIME2","SMALLDATETIME"],
        "snowflake": ["TIMESTAMP_NTZ"]
    },

    "TIMESTAMP_TZ": {
        "oracle": ["TIMESTAMP WITH TIME ZONE"],
        "sql_server": ["DATETIMEOFFSET"],
        "snowflake": ["TIMESTAMP_TZ"]
    },

    "DATE": {
        "oracle": ["DATE"],
        "sql_server": ["DATE"],
        "snowflake": ["DATE"]
    },
    "TIME":{
        "oracle": ["TIME"],
    "sql_server": ["TIME"]
    },
    "NUMBER": {
        "oracle": ["NUMBER","INTEGER","FLOAT"],
        "sql_server": ["INT","SMALLINT","BINARY","BIGINT","DECIMAL","NUMERIC","BIT","TINYINT","FLOAT","MONEY","REAL","SMALLMONEY"],
        "snowflake": ["NUMBER"]
    },
    "VARCHAR": {
        "oracle": ['VARCHAR','CHAR','NVARCHAR','NVARCHAR2'],
        "sql_server": ['VARCHAR','CHAR','NVARCHAR','NVARCHAR2',"NCHAR","TEXT"],
        "snowflake": ['VARCHAR']
    },
    "VARIANT": {
        "oracle": ["VARIANT","BLOB","CLOB"],
        "sql_server": ["VARIANT","BLOB","VARBINARY","IMAGE","XML","SQL","GEOMETRY","GEOGRAPHY","HIERARCHYID"],
        "snowflake": ["VARIANT"]
    }
}

AVAL_MAP_SNOW = {
        "snowflake":["oracle","sql_server","snowflake"]
    }

create_mode = "replace"
l0_rn_table_suffix = "ST"
L1_rn_table_suffix = "ODS"
