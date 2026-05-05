INSTALL spatial;
INSTALL httpfs;

LOAD spatial;
LOAD httpfs;

CREATE OR REPLACE TABLE geo_data AS
SELECT * FROM ST_Read('D:/учёба/для лаб/gis-2026/map.geojson');

CREATE OR REPLACE TABLE overture_buildings_polygons AS
                  SELECT *
                  FROM read_parquet(
                      's3://overturemaps-us-west-2/release/2026-04-15.0/theme=buildings/type=building/*.parquet',
                      hive_partitioning = true
                  )
                  WHERE bbox.xmin <= 50.652266800770775
                    AND bbox.xmax >= 50.63810100748722
                    AND bbox.ymin <= 53.27309762671172
                    AND bbox.ymax >= 53.26603401278885;
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