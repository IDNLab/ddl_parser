import streamlit as st
import pandas as pd
from info_ddl import get_columns_info
import config

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
        df = get_columns_info(ddl_text)

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
st.selectbox(f"Scegli un sistema destinatario", options=config.Aval_opt)
st.selectbox(f"Scegli un sistema sorgente", options=config.Aval_opt)
