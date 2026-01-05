SNOWFLAKE_TYPE_MAP = {
    "TIMESTAMP_NTZ": {
        "oracle": ["TIMESTAMP"],
        "sqlserver": ["DATETIME","DATETIME2","SMALLDATETIME"],
        "snowflake": ["TIMESTAMP_NTZ"]
    },

    "TIMESTAMP_TZ": {
        "oracle": ["TIMESTAMP WITH TIME ZONE"],
        "sqlserver": ["DATETIMEOFFSET"],
        "snowflake": ["TIMESTAMP_TZ"]
    },

    "DATE": {
        "oracle": ["DATE"],
        "sqlserver": ["DATE"],
        "snowflake": ["DATE"]
    },

    "NUMBER": {
        "oracle": ["NUMBER","INTEGER","FLOAT"],
        "sqlserver": ["INT","SNALLINT","BINARY","BIGINT","DECIMAL","NUMERIC"],
        "snowflake": ["NUMBER"]
    },
    "VARCHAR": {
        "oracle": ['VARCHAR','CHAR','NVARCHAR','NVARCHAR2'],
        "sqlserver": ['VARCHAR','CHAR','NVARCHAR','NVARCHAR2'],
        "snowflake": ['VARCHAR','CHAR']
    },
    "VARIANT": {
        "oracle": ["VARIANT","BLOB","CLOB"],
        "sqlserver": ["VARIANT","BLOB"],
        "snowflake": ["VARIANT"]
    }
}
