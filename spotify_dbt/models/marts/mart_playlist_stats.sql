SELECT
    p.PID,
    p.PLAYLIST_NAME,
    p.NUM_TRACKS,
    p.NUM_ARTISTS,
    p.NUM_FOLLOWERS,
    p.DURATION_MIN,
    p.COLLABORATIVE,
    p.MODIFIED_AT_TS,
    COUNT(DISTINCT f.ARTIST_URI)        AS UNIQUE_ARTISTS_ACTUAL,
    AVG(t.DURATION_MIN)                 AS AVG_TRACK_DURATION_MIN
FROM {{ ref('stg_playlists') }}         p
JOIN {{ ref('stg_playlist_tracks') }}   f ON p.PID = f.PID
JOIN {{ ref('stg_tracks') }}            t ON f.TRACK_URI = t.TRACK_URI
GROUP BY 1,2,3,4,5,6,7,8