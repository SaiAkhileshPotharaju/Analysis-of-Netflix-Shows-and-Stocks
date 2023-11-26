SELECT
    s.show_id AS show_id_dim,
    s.type,
    s.title,
    s.cast,
    s.director,
    COUNT(DISTINCT s.cast) AS cast_count,
    s.country,
    AVG(s.duration) AS avg_duration,
    s.description,
    o.original_title,
    SUM(o.seasons) AS total_seasons,
    AVG(o.length) AS avg_length,
    COUNT(DISTINCT o.regions) AS region_count,
    o.status,
    o.length,
    d.date_id AS date_id_dim,
    d.date,
    d.year,
    g.listed_in,
    f.rating,
    AVG(f.rating) AS avg_rating

FROM
    show_dim s
LEFT JOIN
    facts_imdb_rating f ON s.show_id = f.show_id
LEFT JOIN
    original_dim o ON f.original_title = o.original_title
LEFT JOIN
    date_dim d ON f.date_id = d.date_id
LEFT JOIN
    genre_dim g ON f.listed_in = g.listed_in

GROUP BY
    s.show_id,
    s.type,
    s.title,
    s.director,
    s.country,
    s.description,
    o.original_title,
    o.status,
    d.date_id,
    d.date,
    d.year,
    g.listed_in