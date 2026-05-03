INSTALL spatial;
INSTALL httpfs;

LOAD spatial;
LOAD httpfs;

CREATE OR REPLACE TABLE osm_data AS
SELECT * FROM ST_Read('D:/учёба/для лаб/gis-2026/map.geojson');

CREATE OR REPLACE TABLE links AS
WITH raw_data AS (
    SELECT *
    FROM 'https://stac.overturemaps.org/2026-04-15.0/buildings/building/collection.json'
),
raw_links AS (
    SELECT unnest(links) AS link
    FROM raw_data
),
links AS (
    SELECT row_number() OVER () id, link.href
    FROM raw_links
    WHERE link.type = 'application/geo+json'
),
raw_bboxes AS (
    SELECT unnest(extent.spatial.bbox) bbox
    FROM raw_data
),
bboxes AS (
    SELECT row_number() OVER () id, bbox[1] xmin, bbox[2] ymin, bbox[3] xmax, bbox[4] ymax
    FROM raw_bboxes
)
SELECT href, xmin, ymin, xmax, ymax
FROM links
JOIN bboxes ON links.id = bboxes.id;

SET VARIABLE item_url = (
    SELECT DISTINCT
        'https://stac.overturemaps.org/2026-04-15.0/buildings/building/' || links.href
    FROM links
    JOIN osm_data
        ON ST_Xmin(geom) BETWEEN links.xmin AND links.xmax
        AND ST_Ymin(geom) BETWEEN links.ymin AND links.ymax
    LIMIT 1
);

SET VARIABLE s3_href = (
    SELECT assets.aws.alternate.s3.href
    FROM read_json(getvariable('item_url'))
);

CREATE OR REPLACE TABLE overture_buildings AS
WITH osm_data_geom_bbox AS (
    SELECT ST_Extent_Agg(geom) geom
    FROM osm_data
),
osm_data_bbox AS (
    SELECT ST_Xmin(geom) AS xmin,
           ST_Ymin(geom) AS ymin,
           ST_Xmax(geom) AS xmax,
           ST_Ymax(geom) AS ymax
    FROM osm_data_geom_bbox
)
SELECT * EXCLUDE geometry, geometry
FROM read_parquet(getvariable('s3_href')) data
JOIN osm_data_bbox
    ON ST_Xmin(geometry) BETWEEN osm_data_bbox.xmin AND osm_data_bbox.xmax
    AND ST_Ymin(geometry) BETWEEN osm_data_bbox.ymin AND osm_data_bbox.ymax
WHERE try(ST_IsValid(geometry)) = true;

COPY (
    SELECT json_object(
        'type', 'FeatureCollection',
        'features', json_group_array(
            json_object(
                'type', 'Feature',
                'geometry', ST_AsGeoJSON(ST_SetCRS(geometry, 'EPSG:4326'))::JSON,
                'properties', json_object(
                    'id', id,
                    'source_type', source_type,
                    'class', class,
                    'height', height
                )
            )
        )
    )
    FROM (
        SELECT DISTINCT ON (temp.id)
            temp.geometry,
            temp.id,
            temp.class,
            temp.height,
            CASE
                WHEN osm.geom IS NOT NULL THEN 'my'
                WHEN temp.sources->>'$[0].dataset' LIKE 'OpenStreetMap' THEN 'osm'
                ELSE 'ml'
            END AS source_type
        FROM overture_buildings_polygons temp
        LEFT JOIN geo_data osm
            ON try(ST_Intersects(osm.geom, ST_SetCRS(temp.geometry, 'EPSG:4326'))) = true
    )
)
TO 'D:\учёба\для лаб\gis-2026\lab2\client\public\overture.json'
WITH (FORMAT CSV, HEADER false, QUOTE '');