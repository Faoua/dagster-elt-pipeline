import streamlit as st
import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

st.set_page_config(layout="wide")
st.title("🎧 Analyse avancée — Top 15 artistes du classement Deezer")

# Connexion DuckDB
con = duckdb.connect("deezer_top_tracks.db")
df = con.execute("SELECT * FROM top_tracks").fetchdf()

# Limiter à 15 artistes les plus présents
top_artists = df["artist"].value_counts().head(15).index.tolist()
df_top15 = df[df["artist"].isin(top_artists)]

# Layout
col1, col2 = st.columns(2)

# 1. Nombre de morceaux par artiste
with col1:
    st.subheader("🎵 Nombre de morceaux par artiste")
    count_df = df_top15["artist"].value_counts().sort_values()
    fig1, ax1 = plt.subplots(figsize=(8, 6))
    count_df.plot(kind="barh", ax=ax1, color="skyblue")
    ax1.set_xlabel("Nombre de morceaux")
    plt.tight_layout()
    st.pyplot(fig1)

# 2. Durée totale par artiste
with col2:
    st.subheader("⏱ Durée totale par artiste (en minutes)")
    duration_df = df_top15.groupby("artist")["duration"].sum() / 60
    fig2, ax2 = plt.subplots(figsize=(8, 6))
    duration_df.sort_values().plot(kind="barh", ax=ax2, color="salmon")
    ax2.set_xlabel("Minutes")
    plt.tight_layout()
    st.pyplot(fig2)

 

# 4. Classement moyen par artiste
st.subheader("🏅 Classement moyen des artistes")
avg_rank_df = df_top15.groupby("artist")["rank"].mean().sort_values()
fig4, ax4 = plt.subplots(figsize=(10, 6))
avg_rank_df.plot(kind="bar", ax=ax4, color="green")
ax4.set_ylabel("Rang moyen")
ax4.set_xticklabels(avg_rank_df.index, rotation=45, ha='right')
plt.tight_layout()
st.pyplot(fig4)



# 6. Affichage interactif par artiste
st.subheader("🔍 Détail par artiste")
selected_artist = st.selectbox("Choisis un artiste à explorer :", top_artists)
artist_df = df[df["artist"] == selected_artist]
st.dataframe(artist_df[["title", "album", "rank", "duration", "explicit"]])

# 7. Histogramme durée des morceaux (artiste sélectionné)
st.subheader(f"⏳ Durée des morceaux de {selected_artist}")
fig6, ax6 = plt.subplots(figsize=(10, 5))
sns.histplot(artist_df["duration"], bins=8, kde=True, color="orange", ax=ax6)
plt.tight_layout()
st.pyplot(fig6)


# Préparation des données
artist_counts = df["artist"].value_counts()
top_n = 10
artist_counts_top = artist_counts.head(top_n)
autres = artist_counts[top_n:].sum()
if autres > 0:
    artist_counts_top["Autres"] = autres

# Colonne centrale pour centrage
col1, col2, col3 = st.columns([1, 2, 1])

with col2:
    fig, ax = plt.subplots(figsize=(10, 10))  # carré large
    ax.pie(
        artist_counts_top.values,
        labels=artist_counts_top.index,
        autopct="%1.1f%%",
        startangle=90,
        counterclock=False,
        textprops={"fontsize": 11}
    )
    ax.set(aspect="equal")  # forcer un cercle parfait
    plt.tight_layout()
    st.pyplot(fig)


st.subheader("💿 Top albums les plus fréquents dans le Top Deezer")

# Compter les albums
top_albums = df["album"].value_counts().head(15)

# Barplot
fig, ax = plt.subplots(figsize=(10, 5))  # 👈 DEZOOM ici
sns.barplot(y=top_albums.index, x=top_albums.values, palette="magma", ax=ax)

# Personnalisation
ax.set_xlabel("Nombre de morceaux", fontsize=12)
ax.set_ylabel("Album", fontsize=12)
ax.set_title("Top 15 des albums les plus représentés", fontsize=14)
ax.tick_params(axis='y', labelsize=10)
ax.tick_params(axis='x', labelsize=10)

plt.tight_layout()  # 👈 pour éviter que le texte dépasse
st.pyplot(fig)

st.subheader("💿 Top albums (graphe carré & dézoommé)")

# Compter les albums
top_albums = df["album"].value_counts().head(15)

# FIGURE carrée + clean
fig, ax = plt.subplots(figsize=(10, 10))  # 👈 CARRÉ ici (largeur = hauteur)
sns.barplot(
    y=top_albums.index,
    x=top_albums.values,
    palette="rocket",
    ax=ax
)

import plotly.express as px

st.subheader("🎈 Visualisation musicale — Bubble Chart")

# Filtrage des données (optionnel)
df_bubble = df.copy()
df_bubble["explicit_label"] = df_bubble["explicit"].replace({True: "Explicite", False: "Non explicite"})

# Bubble chart
fig = px.scatter(
    df_bubble,
    x="rank",
    y="artist",  # ou "title" si tu préfères
    size="duration",
    color="explicit_label",
    hover_name="title",
    size_max=40,
    color_discrete_map={"Explicite": "red", "Non explicite": "green"},
    title="🎵 Bulles musicales : Durée vs Rang vs Explicite"
)

fig.update_layout(
    xaxis_title="Classement (rank)",
    yaxis_title="Artiste",
    height=700,
    template="plotly_dark"
)

st.plotly_chart(fig, use_container_width=True)


st.subheader("📊 Score de popularité (ranking + durée)")

# Calcul du score personnalisé
df["score"] = (100 - df["rank"]) + (df["duration"] / 30)

top_scored = df.sort_values("score", ascending=False)[["title", "artist", "rank", "duration", "score"]].head(10)
st.dataframe(top_scored)

# Visualisation
fig, ax = plt.subplots(figsize=(10, 6))
sns.barplot(x="score", y="title", data=top_scored, palette="viridis", ax=ax)
ax.set_title("🏆 Titres avec le meilleur score combiné (rank + durée)")
st.pyplot(fig)





st.subheader("📥 Exporter les données du Top Deezer")
csv = df.to_csv(index=False)
st.download_button("⬇️ Télécharger le fichier CSV", data=csv, file_name="deezer_top_tracks.csv", mime='text/csv')
