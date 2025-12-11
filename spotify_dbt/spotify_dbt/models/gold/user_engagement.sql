SELECT 
    user_id,
    device_type,
    country,
    COUNT(CASE WHEN event_type = 'play' THEN 1 END) as plays,
    COUNT(CASE WHEN event_type = 'skip' THEN 1 END) as skips,
    COUNT(CASE WHEN event_type = 'add_to_playlist' THEN 1 END) as playlist_adds,
    DATE_TRUNC('day', event_ts) AS day
from   {{ ref('spotify_silver') }}
GROUP BY user_id, device_type ,country, DATE_TRUNC('day', event_ts)
ORDER BY plays DESC