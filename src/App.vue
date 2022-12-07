<template>
  <div class="header">
    <h1 class="title">UCR Big Data - U.S. Spending Map</h1>
    <h2 class="subtitle">{{ projectTitle }}</h2>
    <h2 class="subtitle">{{ contributorTitle }}</h2>
    <div class="contributors">
      <p v-for="contributor in contributors" :key="contributor">
        {{ contributor }}
      </p>
    </div>
  </div>
  <div class="map-container">
    <h3 class="subtitle map-title">U.S. Spending by County - Hover over to view awards by county</h3>
    <div class="map-wrap">
      <div id="map" class="map-content"></div>
    </div>
  </div>
  <div class="map-container">
    <h3 class="subtitle map-title">Choroplet Map of State and County Spending - zoom in and out to view awards by State and County</h3>
    <div class="map-wrap">
      <div id="choroplethMap" class="map-content"></div>
    </div>
  </div>
  <div class="footer">
    <h4>Front-end by Brooke Godinez and Rucha Kolhatkar</h4>
  </div>
</template>

<script>

import mapboxgl from "mapbox-gl";
import "mapbox-gl/dist/mapbox-gl.css";
import { onMounted } from "vue";


import json from '../data/gz_2010_us_050_00_5m.json';
import countyAndAward from '../data/CountyAndAward.json';
// import stateAndAward from '../data/AwardByState.json';

import points from '../data/GEOLocations.json'


export default {
  data() {
    return {
      projectTitle: "This website is a part of the Antfella's Fall 2022 CSE 226 final project.",
      contributorTitle: "Project contributors:",
      contributors: ["Ivann De la Cruz", "Rucha Kolhatkar", "Brooke Godinez", "Daniel Murphy", "Jesse Stutsman"]
    }
  },

  setup() {
    onMounted(() => {
      mapboxgl.accessToken =
        "pk.eyJ1IjoiYnJvb2tlZ29kaW5leiIsImEiOiJjbGI0bHNxZHEwMG15M3BvNmlkaXZjbTViIn0.BVhJ6Jdb8bEBOJQd_WJtfQ";
      const bounds = [[-175.812867, 4.217278], [-6.279377, 64.1494853]];




      const map = new mapboxgl.Map({
        container: "map",
        style: "mapbox://styles/mapbox/light-v11",
        center: [-100.4, 38.9],
        zoom: 3,
        maxBounds: bounds
      });

      let hoveredStateId = null;
      map.dragRotate.disable();
      map.touchZoomRotate.disableRotation();

      map.on('load', () => {
        map.addSource('states', {
          'type': 'geojson',
          'data': json,
          'generateId': true
        });

        map.addLayer({
          'id': 'state-fills',
          'type': 'fill',
          'source': 'states',
          'layout': {},
          'paint': {
            'fill-color': '#0d324d',
            'fill-opacity': [
              'case',
              ['boolean', ['feature-state', 'hover'], false],
              1,
              0.5]
          }
        });
        map.addLayer({
          'id': 'state-borders',
          'type': 'line',
          'source': 'states',

          'layout': {},
          'paint': {
            'line-color': '#0d324d',
            'line-width': 1
          }
        });
        const popup = new mapboxgl.Popup({
          closeButton: false,
          closeOnClick: false
        })

        map.on('mousemove', 'state-fills', (e) => {
          if (e.features.length > 0) {
            if (hoveredStateId !== null) {
              map.setFeatureState(
                { source: 'states', id: hoveredStateId },
                { hover: false }
              );
            }

            
            const description = countyAndAward.filter(item => item.county.toLowerCase() == e.features[0].properties.NAME.toLowerCase());
            
            const coordinates = e.lngLat;
            popup.setLngLat(coordinates).setHTML(description[0].award).addTo(map);



            hoveredStateId = e.features[0].id;
            map.setFeatureState(
              { source: 'states', id: hoveredStateId },
              { hover: true }
            );
          }
        });
        map.on('mouseleave', 'state-fills', () => {
          if (hoveredStateId !== null) {
            map.setFeatureState(
              { source: 'states', id: hoveredStateId },
              { hover: false }
            );
          }
          hoveredStateId = null;
          popup.remove();
        });

      });
      // const otherBounds = [[-114.697266, 33.651208], [-68.554688, 15.432915]];
      const choroplethMap = new mapboxgl.Map({
        container: "choroplethMap",
        style: "mapbox://styles/mapbox/light-v11",
        center: [-100.4, 38.9],
        minZoom: 2,
        zoom: 3,
        maxBounds: bounds
      });
      const zoomThreshold = 4;

      choroplethMap.dragRotate.disable();
      choroplethMap.touchZoomRotate.disableRotation();

      choroplethMap.on('load', () => {
        choroplethMap.addSource('county_outline', {
          'type': 'geojson',
          'data': json,
          'generateId': true
        });

        choroplethMap.addSource('state_outline', {
          "type": 'geojson',
          "data": "https://docs.mapbox.com/mapbox-gl-js/assets/us_states.geojson",
          'generateId': true
        });
        
      
        choroplethMap.addSource('points', {
          'type': 'geojson',
          'data': points,
          'generateId': true
        });
        


        choroplethMap.addLayer(
          {
            'id': 'state-boarders',
            'source': 'state_outline',
            // 'source-layer': 'state_county_population_2014_cen',
            'maxzoom': zoomThreshold,
            'type': 'line',
            'layout': {},
            'paint': {
              'line-color': '#0d324d',
              'line-width': 1,
            }
          });

        choroplethMap.addLayer(
          {
            'id': 'county-population',
            'source': 'county_outline',
            // 'source-layer': 'state_county_population_2014_cen',
            'minzoom': zoomThreshold,
            'type': 'line',
            'layout': {},
            'paint': {
              'line-color': '#0d324d',
              'line-width': 1
            }

           });
        choroplethMap.addLayer({
          'id': 'state-fills',
          'type': 'fill',
          'source': 'states',
          'layout': {},
          'paint': {
            'fill-color': '#0d324d',
            'fill-opacity': [
              'case',
              ['boolean', ['feature-state', 'hover'], false],
              1,
              0.5]
          }
        });
        
        choroplethMap.addLayer({
          id: 'points',
          type: 'circle',
          source: 'points', 
          paint: {
            'circle-radius': 5,
            'circle-color': '#0d324d'
          }
        });
        choroplethMap.on('click', 'points', (e) => {
        // Copy coordinates array.
        const coordinates = e.features[0].geometry.coordinates.slice();
        const description = e.features[0].properties.name;
        
        // Ensure that if the map is zoomed out such that multiple
        // copies of the feature are visible, the popup appears
        // over the copy being pointed to.
        while (Math.abs(e.lngLat.lng - coordinates[0]) > 180) {
        coordinates[0] += e.lngLat.lng > coordinates[0] ? 360 : -360;
        }
        
        new mapboxgl.Popup()
        .setLngLat(coordinates)
        .setHTML(description)
        .addTo(choroplethMap);
        });
        choroplethMap.on('mouseenter', 'points', () => {
        choroplethMap.getCanvas().style.cursor = 'pointer';
        });
        
        // Change it back to a pointer when it leaves.
        choroplethMap.on('mouseleave', 'points', () => {
        choroplethMap.getCanvas().style.cursor = '';
});


      });
      return {};
    })
  }
}

</script>

<style>
#app {
  font-family: Avenir, Helvetica, Arial, sans-serif;
  -webkit-font-smoothing: antialiased;
  -moz-osx-font-smoothing: grayscale;
  text-align: center;
  color: #2c3e50;
}

body {
  margin: 0;
  padding: 0;
}

.header {
  background-color: #0d324d;
  padding: 64px;
}

.title {
  color: #fff;
  font-size: 48px;
  margin: 0 0 24px
}

.subtitle {
  color: #fff;
  font-size: 24px;
  margin: 12px 0;
}

.map-container {
  margin-top: 80px;
}

.map-content {
  margin: auto;
  position: center;
  height: 500px;
  width: 100%;
  padding: 10px;
  border: 2px solid #0d324d;
}

.map-wrap {
  margin: 24px 12.5%;
  width: 75%;
}

.map-title {
  color: #2c3e50;
}

.contributors {
  width: 200px;
  margin: 8px auto;
}

.contributors p {
  color: #fff;
  font-size: 18px;
  margin: 8px 0;
}

.legend {
  background-color: #fff;
  border-radius: 3px;
  bottom: 30px;
  box-shadow: 0 1px 2px rgba(0, 0, 0, 0.1);
  font: 12px/20px 'Helvetica Neue', Arial, Helvetica, sans-serif;
  padding: 10px;
  position: absolute;
  right: 10px;
  z-index: 1;
}

.legend h4 {
  margin: 0 0 10px;
}

.legend div span {
  border-radius: 50%;
  display: inline-block;
  height: 10px;
  margin-right: 5px;
  width: 10px;
}

.footer {
  align-items: center;
  background: #000000;
  color: #fff;
  display: flex;
  height: 64px;
  justify-content: center;
  margin-top: 80px;
  width: 100%;
}
</style>
