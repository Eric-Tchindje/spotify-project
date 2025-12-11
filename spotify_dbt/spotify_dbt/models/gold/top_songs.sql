SELECT
    song_id,
    song_name,
    artist_name,
    COUNT(CASE WHEN event_type = 'play' THEN 1 END) as total_plays,
    COUNT(CASE WHEN event_type = 'skip' THEN 1 END) as total_skips
FROM {{ ref('spotify_silver') }}
GROUP BY song_id, song_name, artist_name
ORDER BY total_plays DESC
