import { useEffect, useRef } from "react";

import { View, Map } from "ol";
import TileLayer from "ol/layer/Tile";
import { OSM } from "ol/source";
import ImageLayer from "ol/layer/Image";
import ImageWMS from "ol/source/ImageWMS";

import apply from "ol-mapbox-style";
import mapboxStyle from './assets/styles.json';
import * as olProj from 'ol/proj';
import { getView } from "ol/View";

function App() {
    const mapRef = useRef<Map>(null);

    useEffect(() => {
        if (mapRef.current) return;

        mapRef.current = new Map({
            target: "map",
            layers: [
                new TileLayer({
                    source: new OSM(),
                }),
                new ImageLayer({
                    source: new ImageWMS({
                        url: "http://localhost:8080/geoserver/gis/wms",
                        params: {
                            LAYERS: "gis:buildings",
                            TILED: true,
                        },
                        ratio: 1,
                        serverType: "geoserver",
                    }),
                }),
                new ImageLayer({
                    source: new ImageWMS({
                        url: "http://localhost:8080/geoserver/gis/wms",
                        params: {
                            LAYERS: "gis:roads",
                            TILED: true,
                        },
                        ratio: 1,
                        serverType: "geoserver",
                    }),
                }),
            ],
            view: new View({
                center: [5637889.748140545, 7032983.351178021],
                zoom: 16,
            }),
        });

        apply(mapRef.current, mapboxStyle);

        
        const view = mapRef.current.getView();

        view.on('change', () => {
            const ext = mapRef.current?.getView().calculateExtent();
            if (ext) console.log(olProj.transformExtent(ext,  mapRef.current?.getView().getProjection(), 'EPSG:4326'));
        })

    }, []);

    return <div id="map" />;
}

export default App;
