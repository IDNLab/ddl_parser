import streamlit as st
from parser_core.info_ddl_oop import DDLInfo
from mainconfig import config
from mainconfig.datatype_map import SNOWFLAKE_TYPE_MAP

def map_to_target_type(source_type: str, source_system: str) -> str:
    source_system = source_system.lower()
    source_type = source_type.upper()  # opzionale ma consigliato

    for snowflake_type, system_map in SNOWFLAKE_TYPE_MAP.items():
        if source_system in system_map:
            if source_type in system_map[source_system]:
                return snowflake_type

    return "UNMAPPED"


st.set_page_config(page_title="DDL Parser Viewer", layout="wide")

st.title("DDL Parser")

# input DDL
ddl_text = st.text_area(
    "Incolla qui la DDL SQL",
    height=150,
    placeholder="CREATE TABLE ecc ecc..."
)

if ddl_text.strip():

    try:
        # il tuo parser deve restituire un DataFrame
        df = DDLInfo(ddl_text).to_dataframe()

        st.subheader("DataFrame restituito dal parser")
        st.dataframe(df, use_container_width=True)

        if "column_name" not in df.columns:
            st.error("La colonna 'column_name' non è presente nel DataFrame")
        else:
            # estrazione dei nomi
            column_names = (
                df["column_name"]
                .dropna()
                .astype(str)
                .tolist()
            )

            st.subheader("Nomi estratti da column_name")
            st.write(f"""{", ".join(column_names)}""")

            # opzionale: unici + conteggio
            st.subheader("Statistiche - Matchare con definizione di DDL")
            st.write({
                "totale_righe": len(df),
                "colonne_non_nulle": len(column_names),
                "colonne_uniche": len(set(column_names))
            })

    except Exception as e:
        st.exception(e)
else:
    st.info("Incolla una DDL per avviare il parsing")

st.subheader("CONVERSIONE DDL")
if ddl_text.strip():
    target_system = st.selectbox(f"Scegli un sistema destinatario", options=config.Aval_opt)
    source_system = st.selectbox(f"Scegli un sistema sorgente", options=config.Aval_opt)
    df_modded = df
    df_modded["SNOWFLAKE_DATA_TYPE"] = df_modded["datatype"].apply(
        lambda x: map_to_target_type(x, source_system)
    )
    st.dataframe(df_modded, use_container_width=True)

st.header(f"IMPORT_MASSIVO")