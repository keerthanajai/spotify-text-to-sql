SELECT
    t.TRACK_URI,
    t.TRACK_NAME,
    a.ARTIST_NAME,
    t.ALBUM_NAME,
    t.DURATION_MIN,
    COUNT(DISTINCT f.PID)           AS PLAYLIST_COUNT,
    SUM(p.NUM_FOLLOWERS)            AS TOTAL_FOLLOWER_REACH
FROM {{ ref('stg_tracks') }}            t
JOIN {{ ref('stg_playlist_tracks') }}   f ON t.TRACK_URI = f.TRACK_URI
JOIN {{ ref('stg_artists') }}           a ON t.ARTIST_URI = a.ARTIST_URI
JOIN {{ ref('stg_playlists') }}         p ON f.PID = p.PID
GROUP BY 1, 2, 3, 4, 5
ORDER BY PLAYLIST_COUNT DESC