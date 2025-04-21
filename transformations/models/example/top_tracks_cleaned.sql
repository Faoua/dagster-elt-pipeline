-- transformations/models/top_tracks_cleaned.sql

SELECT
  rank,
  title,
  artist,
  album,
  duration / 60.0 AS duration_minutes,
  explicit,
  link
FROM top_tracks
WHERE explicit = true
ORDER BY rank
