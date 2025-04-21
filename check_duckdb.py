'''
import duckdb

# Connexion à la base
con = duckdb.connect("deezer_top.db")

# Affiche les noms de toutes les tables
print("Tables disponibles :")
tables = con.execute("SHOW TABLES").fetchall()
print(tables)

# Affiche les 5 premiers morceaux
print("\n🎵 Top Tracks :")
print(con.execute("SELECT * FROM top_tracks LIMIT 5").fetchdf())

# Affiche les 5 premiers albums
print("\n💿 Top Albums :")
print(con.execute("SELECT * FROM top_albums LIMIT 5").fetchdf())

# Affiche les 5 premiers artistes
print("\n🎤 Top Artists :")
print(con.execute("SELECT * FROM top_artists LIMIT 20").fetchdf())

# Affiche les 5 premières playlists
print("\n🎧 Top Playlists :")
print(con.execute("SELECT * FROM top_playlists LIMIT 5").fetchdf())

con.close()

print("\n🔍 Tracks cleaned by dbt :")
print(con.execute("SELECT * FROM top_tracks_cleaned LIMIT 5").fetchdf())
'''

'''
import duckdb

con = duckdb.connect("deezer_top.db")

# Regarde si la table a été créée
print("📋 Tables disponibles :")
for table in con.execute("SHOW TABLES").fetchall():
    print("-", table[0])

# Regarde les 5 dernières lignes
print("\n📊 Derniers morceaux insérés (partitionnés) :")
df = con.execute("""
    SELECT * FROM top_tracks_partitioned 
    ORDER BY partition_date DESC 
    LIMIT 5
""").fetchdf()
print(df)

con.close()
'''
import duckdb
con = duckdb.connect("deezer_top.db")
print(con.execute("SELECT DISTINCT partition_date FROM top_tracks_partitioned").fetchdf())


