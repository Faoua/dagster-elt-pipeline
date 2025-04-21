SELECT
  rank,
  name,
  nb_fans
FROM top_artists
WHERE nb_fans IS NOT NULL AND nb_fans > 100000
ORDER BY nb_fans DESC
