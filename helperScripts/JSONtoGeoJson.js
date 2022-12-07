const fs = require("fs");

let raw_locationsJSON = fs.readFileSync('/Users/brooke/big_data/data/Locations.json');

let locationsJSON = JSON.parse(raw_locationsJSON);
// let geoJSON = {
//     type: 'Feature',
//     geometry: {
//         type: "Point",
//         coordinates: []
//     },
//     properties: {}
// }

let geoJSON = []

for (let i = 1; i < locationsJSON.length; i++)
{
    let tempJSON = {};
    tempJSON.type = "Feature";
    let geo = {}
    tempJSON.geometry = geo
    tempJSON.geometry.type = "Point"
    tempJSON.geometry.coordinates = [parseFloat(locationsJSON[i].long), parseFloat(locationsJSON[i].lat)];
    let prop = {}
    tempJSON.properties = prop;
    tempJSON.properties.name = locationsJSON[i].R_COUNTY
    geoJSON.push(tempJSON)
}
let trueGeoJSON = {}

trueGeoJSON.type = "FeatureCollection"
trueGeoJSON.features = geoJSON


let json = JSON.stringify(trueGeoJSON);
fs.writeFileSync('data/GEOLocations.json', json);
console.log("file written sucessfully")