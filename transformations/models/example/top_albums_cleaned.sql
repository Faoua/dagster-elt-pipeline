SELECT
  rank,
  title,
  artist,
  nb_tracks,
  CAST(SUBSTR(CAST(release_date AS VARCHAR), 1, 4) AS INTEGER) AS release_year,
  link
FROM top_albums
WHERE nb_tracks IS NOT NULL
ORDER BY rank
