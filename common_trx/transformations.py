import logging
from datetime import datetime
import os
from mainconfig import config

log_dir = config.log_dir
today = datetime.now().strftime("%Y%m%d")
logger = logging.getLogger("transformations")
log_file = f"{log_dir}/{config.log_filename}_{today}.log"
logger.setLevel(logging.DEBUG)
os.makedirs(log_dir, exist_ok=True)


def build_reverse_typemap(typemap: dict, source_system: str) -> dict:
    reverse = {}
    logger.debug(f"|build_reverse_typemap| , richiamato reverse_typemap per il source system:{source_system}")
    logger.debug(f"|build_reverse_typemap| , avvio ciclo di matching per :{source_system}")
    for target_type, systems in typemap.items():
        if source_system not in systems:
            logger.debug(f"|build_reverse_typemap| , ciclato source system non corrispondente:{source_system} : {systems}")
            continue

        for src_type in systems[source_system]:
            logger.debug(
                f"|build_reverse_typemap| , ciclato source system corrispondente:{source_system} : {systems}")
            reverse[src_type.upper()] = target_type
    return reverse

def generate_additional_dtype_cols(df_source, reverse_typemap: dict):
    logger.debug("|generate_additional_dtype_cols| aggiunta datatype Snowflake")

    if df_source.empty:
        logger.error("|generate_additional_dtype_cols| DataFrame vuoto")
        raise ValueError("DataFrame vuoto: impossibile generare ADDITIONAL_DTYPE")

    if "datatype" not in df_source.columns:
        logger.error("|generate_additional_dtype_cols| Colonna 'datatype' non trovata")
        raise KeyError("Colonna 'datatype' non presente nel DataFrame")

    if not reverse_typemap:
        logger.error("|generate_additional_dtype_cols| reverse_typemap vuoto o None")
        raise ValueError("reverse_typemap non valido")

    df_source[config.add_field_dtype] = (
        df_source["datatype"]
        .astype(str)
        .str.upper()
        .map(reverse_typemap)
    )

    return df_source


def generate_additional_length_cols(df):
    if df.empty:
        logger.error("|generate_additional_length_cols| DataFrame vuoto")
        raise ValueError("DataFrame vuoto: impossibile generare ADDITIONAL_LENGTH")

    if "length" not in df.columns:
        logger.error("|generate_additional_length_cols| Colonna 'length' non trovata")
        raise KeyError("Colonna 'length' non presente nel DataFrame")

    df[config.add_field_length] = (
        df["length"]
        .where(df["length"].notna() & (df["length"] != 0))
    )
    return df

def generate_additional_upper_cols(df):
    if df.empty:
        logger.error("|generate_additional_upper_cols| DataFrame vuoto")
        raise ValueError("DataFrame vuoto: impossibile generare UPPER")
    if "column_name" not in df.columns:
        logger.error(f"generate_additional_upper_cols| Colonna 'column_name' non trovata")
        raise KeyError("Colonna 'column_name' non presente nel DataFrame")
    df[config.add_field_upper] = (df["column_name"].str.upper())
    return df

def get_elements(df, field):
    """
    Estrae una lista di valori da una colonna di un DataFrame Pandas,
    applicando controlli di validità sul contenuto.
    """
    logger.info(f"|get_elements| estraggo lista elementi per {field}")

    if df.empty:
        logger.error("|get_elements| DataFrame vuoto")
        raise ValueError("DataFrame vuoto")

    if field not in df.columns:
        logger.error("|get_elements| Colonna non presente nel DataFrame")
        raise ValueError("Colonna non presente nel DataFrame")

    return df[field].tolist()

def count_df_elements(df, fields):
    logger.debug(f"|count_df_elements| DataFrame in esame per conteggio, sul campo {fields}")
    if df.empty:
        logger.error("|count_df_elements| DataFrame vuoto")
        raise ValueError("DataFrame vuoto")
    count_df = df.groupby(fields).count()
    return count_df