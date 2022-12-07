const fs = require("fs");

csv = fs.readFileSync('/Users/brooke/big_data/LocationsMap.csv');



let array = csv.toString().split('\n');

let result = [];

let headers = array[0].split(',');

// console.log("hello");
console.log(typeof array[1]);


for (let i = 1; i < array.length - 1; i++) {
    let obj = {};
    let str = array[i];
    let s = '';
    let flag = 0
    for (let ch of str) {
        if (ch === '"' && flag === 0) {
        flag = 1
        }
        else if (ch === '"' && flag == 1) flag = 0
        if (ch === ',' && flag === 0) ch = '|'
        if (ch !== '"') s += ch
    }
    let properties = s.split("|")
    for (let j in headers) {
        if (properties[j].includes(",")) {
          obj[headers[j]] = properties[j]
            .split(", ").map(item => item.trim())
        }
        else obj[headers[j]] = properties[j]
      }
    result.push(obj)
}

let json = JSON.stringify(result);
fs.writeFileSync('data/Locations.json', json);
console.log("file written sucessfully")



