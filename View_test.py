import streamlit as st
import plotly.express as px
from repository import *
import pandas as pd  # Assurez-vous que pandas est importé
import traceback  # Importer traceback pour obtenir des erreurs détaillées

# === Configuration de l'application ===
st.set_page_config(
    page_title="Dashboard Multi-Pages",
    layout="wide",
    initial_sidebar_state="expanded"
)

# === Load du Referentiel ===
dataholder = DataHolder()
ref_funds = dataholder.load('referentiel_fond')
funds_list = list(ref_funds['code_ptf'])
manager_list = list(ref_funds['Portfolio Manager'].unique())

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
    st.session_state.setdefault("metrics_data", None)
    st.session_state.setdefault("error_message", None)


# === Gestion de l'état de session ===
st.session_state.setdefault("selected_fund", None)
st.session_state.setdefault("selected_period", None)
st.session_state.setdefault("metrics_data", None)
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
            metrics_data = None

            if mode == "Historique":
                # Vérifier si les données sont déjà en cache
                metrics_data = get_cache_file(dataholder, selected_fund, selected_quarter)

            # Si les données ne sont pas en cache ou si le mode est "Calculatoire", appeler get_metrics
            if metrics_data is None:
                dict_metrics = get_metrics(config, dataholder, selected_fund, selected_quarter)
                metrics_data = dict_metrics['METRICS']

            st.session_state["metrics_data"] = metrics_data
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
        st.warning("Paramètres réinitialisés. Veuillez configurer de nouveaux paramètres.")

    # Affichage du message d'erreur s'il y en a un
    if st.session_state["error_message"]:
        with st.expander("Afficher les détails de l'erreur"):
            st.error(f"Erreur : {st.session_state['error_message']}")

# === Page de présentation ===
elif menu == "Page de présentation":
    if not st.session_state["selected_fund"] or not st.session_state["selected_period"]:
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
        st.markdown("Cette section peut inclure des graphiques ou des éléments visuels supplémentaires.")

# === Page des graphes et tableaux 1 ===
elif menu == "Dashboard - 1":
    if not st.session_state["selected_fund"] or not st.session_state["selected_period"]:
        st.title("Graphes et Tableaux - Partie 1")
        st.warning("Les paramètres ont été réinitialisés. Veuillez configurer les paramètres dans la page d'accueil.")
    else:
        st.title("Graphes et Tableaux - Partie 1")
        st.write("Cette page contient des graphiques et des tableaux.")

        # Exemple de graphiques en camembert
        st.subheader("Graphiques en camembert")

        metrics_data = st.session_state["metrics_data"]

        if metrics_data is not None:
            metrics_df = metrics_data

            # Créer une nouvelle colonne pour les labels
            metrics_df['label'] = metrics_df['delta_pnl'].apply(
                lambda x: 'Bonnes' if x > 0 else ('Mauvaises' if x < 0 else 'Neutres'))

            # Compter le nombre de bonnes, mauvaises et neutres actives managements
            count_labels = metrics_df['label'].value_counts().reset_index()
            count_labels.columns = ['label', 'count']

            # Calculer la somme des delta_pnl pour chaque catégorie
            sum_delta_pnl = metrics_df.groupby('label')['delta_pnl'].sum().reset_index()
            sum_delta_pnl.columns = ['label', 'total_delta_pnl']

            # Fusionner les deux dataframes
            summary = pd.merge(count_labels, sum_delta_pnl, on='label')

            # Créer le pie chart
            fig1 = px.pie(summary, names='label', values='count', title="Nombre d'Actives Managements",
                          hover_data={'total_delta_pnl': True}, labels={'total_delta_pnl': 'Total Delta PnL'})

            # Afficher le graphique dans Streamlit
            st.plotly_chart(fig1)

# === Page des graphes et tableaux 2 ===
elif menu == "Dashboard - 2":
    if not st.session_state["selected_fund"] or not st.session_state["selected_period"]:
        st.title("Graphes et Tableaux - Partie 2")
        st.warning("Les paramètres ont été réinitialisés. Veuillez configurer les paramètres dans la page d'accueil.")
    else:
        st.title("Graphes et Tableaux - Partie 2")
        st.write("Cette page contient des tableaux supplémentaires.")

        # Exemple de tableaux supplémentaires
        st.subheader("Tableaux supplémentaires")
        table_data_2 = pd.DataFrame({
            "Colonne 1": ["F", "G", "H", "I", "J"],
            "Colonne 2": [110, 120, 130, 140, 150],
            "Colonne 3": [210, 220, 230, 240, 250],
            "Colonne 4": [11, 12, 13, 14, 15],
            "Colonne 5": [15, 14, 13, 12, 11]
        })
        st.table(table_data_2.head(5))