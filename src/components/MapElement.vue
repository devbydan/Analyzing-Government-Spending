<template>
    <header>
        <h1>
            <div id="mapContainer"></div>
        </h1>
    </header>
</template>



<script>

    // import "leaflet/dist/leaflet.css";
    // import L from "leaflet";
    import mapboxgl from "mapbox-gl";
    import "mapbox-gl/dist/mapbox-gl.css";
    // import { onMounted } from "vue";


    export default{
        name: "MapElement",
        data(){
            return {
                center: [41.759, -88.157]
            }
        }, 
        // metaInfo : {
        //     script: [
        //         {src: "https://unpkg.com/esri-leaflet@^3.0.8/dist/esri-leaflet.js"},
        //         {src: "https://unpkg.com/esri-leaflet-vector@4.0.0/dist/esri-leaflet-vector.js"},
        //         {src: "https://unpkg.com/leaflet@1.8.0/dist/leaflet.js"}
        //     ],
        // },

        methods: {
            setUpLeafletMap: function() {
                mapboxgl.accessToken = "pk.eyJ1IjoiYnJvb2tlZ29kaW5leiIsImEiOiJjbGI0bHNxZHEwMG15M3BvNmlkaXZjbTViIn0.BVhJ6Jdb8bEBOJQd_WJtfQ";
                const map = new mapboxgl.Map({
                container: "mapContainer",
                style: "mapbox://styles/mapbox/light-v9",
            });

                // const apiKey = "AAPKfada2e4ca8a146aabcd2a125fd1a576dkkBXMI0a5J9lm6OPTz7GKYi3FfaLNVdb8qHDkvlheRvMx9rzWPaCWU8Q7GLWwqlW";
                // const mapDiv = L.map("mapContainer").setView(this.center, 12);

                // L.esri.Vector.vectorBasemapLayer("ArcGIS:LightGray", {
                //     apikey: apiKey
                // }).addTo(mapDiv);
                
                // L.esri.Heat.featureLayer({
                //     url: "https://sampleserver6.arcgisonline.com/arcgis/rest/services/CommunityAddressing/MapServer/0",
                //     radius: 10
                // }).addTo(mapDiv);
            }                
        },

        mounted(){
            this.setUpLeafletMap();
        }
    }
</script>

<style scoped>
#mapContainer {height: 100px;}
</style>