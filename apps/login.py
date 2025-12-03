import streamlit as st

def login(PASSWORD):
    st.title("🔐 Acceso Restringido")

    pwd = st.text_input("Contraseña", type="password")

    if st.button("Entrar"):
        if pwd == PASSWORD:
            st.session_state["logged"] = True
            st.rerun()
        else:
            st.error("Contraseña incorrecta")