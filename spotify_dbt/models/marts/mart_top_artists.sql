SELECT
    a.ARTIST_URI,
    a.ARTIST_NAME,
    COUNT(DISTINCT f.PID)           AS PLAYLIST_COUNT,
    COUNT(DISTINCT f.TRACK_URI)     AS UNIQUE_TRACKS,
    SUM(p.NUM_FOLLOWERS)            AS TOTAL_FOLLOWER_REACH,
    AVG(p.NUM_FOLLOWERS)            AS AVG_PLAYLIST_FOLLOWERS
FROM {{ ref('stg_artists') }}           a
JOIN {{ ref('stg_playlist_tracks') }}   f ON a.ARTIST_URI = f.ARTIST_URI
JOIN {{ ref('stg_playlists') }}         p ON f.PID = p.PID
GROUP BY 1, 2
ORDER BY PLAYLIST_COUNT DESC