WITH bronze_data AS (
    SELECT
        event_id,
        user_id,
        song_id,
        artist_name,
        song_name,
        event_type,
        device_type,
        country,
        TRY_TO_TIMESTAMP(timestamp) AS event_ts
    FROM {{ source('bronze', 'spotify_events_bronze') }}
)

select 
   *
from bronze_data
where event_ts is not null
and event_id is not null
and user_id is not null     
and song_id is not null