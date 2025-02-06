import streamlit as st
import plotly.express as px

from repository import *
import pandas as pd
import traceback

# === Configuration de l'app ===
st.set_page_config(
    page_title="Dashboard Multi-Pages",
    layout="wide",
    initial_sidebar_state="expanded"
)

# === Load du Referentiel ===
dataholder = DataHolder()
ref_funds = dataholder.load('referentiel_fund')
funds_list = list(ref_funds['code_ptf'])

# === Load des Valeurs Config Toml ===
config = Config()
quarters = list(config.hold['QuarterlyExpo'].keys())


# === Fonction pour réinitialiser les paramètres ===
def reset_session_state():
    # Supprimer toutes les clés de session
    for key in st.session_state.keys():
        del st.session_state[key]

    # Réinitialiser les clés importantes (pour éviter des erreurs dans le code)
    st.session_state.setdefault("selected_fund", None)
    st.session_state.setdefault("selected_period", None)
    st.session_state.setdefault("mode","Historical")
    st.session_state.setdefault("metrics_data", None)
    st.session_state.setdefault('many_pockets',None)
    st.session_state.setdefault("merged_metrics", None)
    st.session_state.setdefault("error_message", None)


# === Gestion de l'état de session ===
st.session_state.setdefault("selected_fund", None)
st.session_state.setdefault("selected_period", None)
st.session_state.setdefault("mode","Historical")
st.session_state.setdefault("metrics_data", None)
st.session_state.setdefault('many_pockets',None)
st.session_state.setdefault("merged_metrics", None)
st.session_state.setdefault("error_message", None)

# === Menu principal avec plusieurs pages ===
menu = st.sidebar.radio("Navigation", ["Page d'accueil", "Page de présentation", "Dashboard - 1", "Dashboard - 2"])

# === Vérification des paramètres avant d'afficher une page autre que la page d'accueil ===
if menu != "Page d'accueil" and (not st.session_state["selected_fund"] or not st.session_state["selected_period"]):
    st.sidebar.warning("Veuillez configurer les paramètres dans la page d'accueil.")
    st.error("Vous n'avez pas encore configuré les paramètres requis. Veuillez revenir à la page d'accueil.")
    st.stop()  # Stoppe l'exécution du reste de la page si les paramètres ne sont pas définis

# === Page d'accueil ===
if menu == "Page d'accueil":
    st.title("Bienvenue sur le Dashboard d'Analyse")
    st.write("Cette page vous permet de configurer les paramètres pour le dashboard du fond que vous voulez analyser.")

    # Sélection du fond
    st.subheader("Choisissez un fond")
    selected_fund = st.selectbox(
        "Sélectionnez un fond parmi la liste ci-dessous :",
        funds_list
    )

    # Sélection de la période
    st.subheader("Choisissez une période")
    selected_quarter = st.selectbox(
        "Choisissez un trimestre :",
        quarters
    )

    st.subheader("Choisissez un mode")
    mode = st.selectbox("Mode :", ["Historique", "Calculatoire"])

    # Bouton pour sauvegarder les paramètres
    if st.button("Enregistrer les paramètres"):
        try:
            referentials = ref_funds[ref_funds['code_ptf'] == selected_fund]
            pockets = referentials['pocket_code'].iloc[0]
            pockets = [pocket.strip() for pocket in pockets.split(",")] if "," in pockets else [pockets.strip()]
            nb_pockets = len(pockets)
            dict_metrics = {pocket: {'METRICS':None} for pocket in pockets}

            #Si Histo alors on verifie le cache
            if mode == "Historique":
                for pocket in pockets:
                    dict_metrics[pocket]['METRICS'] = get_cache_file(dataholder, pocket, selected_quarter)

            # Vérifier si toutes les poches ont des données
            data_fetched = all(dict_metrics[pocket]['METRICS'] is not None for pocket in pockets)

            # Si Calculatoire ou not in cache alors on utilise get metrics
            if not data_fetched or mode == "Calculatoire":
                dict_metrics = get_metrics(config, dataholder, selected_fund, selected_quarter)

            if nb_pockets> 1:
                merged_metrics = pd.concat([dict_pocket['METRICS'] for dict_pocket in dict_metrics.values()],ignore_index=True)
                many_pockets = True
            else:
                merged_metrics = dict_metrics[selected_fund]['METRICS']
                many_pockets = False

            st.session_state["metrics_data"] = dict_metrics
            st.session_state['merged_metrics'] = merged_metrics
            st.session_state['many_pockets'] = many_pockets
            st.session_state["selected_fund"] = selected_fund
            st.session_state["selected_period"] = selected_quarter
            st.session_state["mode"] = mode
            st.session_state["error_message"] = None
            st.success("Paramètres enregistrés avec succès !")

        except Exception as e:
            error_message = traceback.format_exc()
            st.session_state["error_message"] = error_message

    # Bouton pour réinitialiser les paramètres
    if st.button("Réinitialiser les paramètres"):
        reset_session_state()
        st.warning("Paramètres réinitialisés. Veuillez configurer à nouveau des paramètres.")

    # Affichage du message d'erreur s'il y en a un
    if st.session_state["error_message"]:
        with st.expander("Afficher les détails de l'erreur"):
            st.error(f"Erreur : {st.session_state['error_message']}")

# === Page de présentation ===
elif menu == "Page de présentation":
    if not st.session_state["selected_fund"] or not st.session_state["selected_period"] or not st.session_state['mode']:
        st.title("Page de Présentation")
        st.warning("Les paramètres ont été réinitialisés. Veuillez configurer les paramètres dans la page d'accueil.")
    else:
        st.title("Dashboard d'Analyse")
        st.write("Cette page présente une vue générale avec les paramètres sélectionnés.")

        # Affichage du titre
        st.markdown(f"### Dashboard d'Analyse du Fond : **{st.session_state['selected_fund']}**")

        # Affichage de la période
        st.markdown(f"#### Période : {st.session_state['selected_period']}")

        # Présentation visuelle avec des barres
        st.markdown("---")
        st.markdown("Cette section peut include des graphiques ou des éléments visuels supplémentaires.")

# === Page des graphes et tableaux 1 ===
elif menu == "Dashboard - 1":
    if not st.session_state["selected_fund"] or not st.session_state["selected_period"] or not st.session_state['mode']:
        st.title("Graphes et Tableaux - Partie 1")
        st.warning("Les paramètres ont été réinitialisés. Veuillez configurer les paramètres dans la page d'accueil.")
    else:
        st.title("Graphes et Tableaux - Partie 1")
        st.write("Cette page contient des graphiques et des tableaux.")

        # Exemple de graphiques en camembert
        st.subheader("Graphiques en camembert")

        metrics_data = st.session_state["metrics_data"]
        merged_metrics = st.session_state['merged_metrics']

        st.dataframe(merged_metrics)


# === Page des graphes et tableaux 2 ===
elif menu == "Dashboard - 2":
    if not st.session_state["selected_fund"] or not st.session_state["selected_period"] or not st.session_state['mode']:
        st.title("Graphes et Tableaux - Partie 2")
        st.warning("Les paramètres ont été réinitialisés. Veuillez configurer les paramètres dans la page d'accueil.")
    else:
        st.title("Graphes et Tableaux - Partie 2")
        st.write("Cette page contient des tableaux supplémentaires.")
