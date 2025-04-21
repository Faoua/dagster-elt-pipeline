SELECT
  rank,
  title,
  nb_tracks,
  user
FROM top_playlists
WHERE nb_tracks >= 10
ORDER BY rank
