
const fs = require("fs");

let rawZip = fs.readFileSync('data/USCities.json');
let rawBigData = fs.readFileSync('data/cpas.json');

let zipCodes = JSON.parse(rawZip);
let bigData = JSON.parse(rawBigData);


for (let i = 0; i < zipCodes.length; i++){
    // for(key in bigData){
    //     if ()
    // }
    let totalAward = 0;
    let obj = zipCodes[i];
    for (let j = 0; j < bigData.length; j++){
        let obj2 = bigData[j];
        
        const tempStr = obj2.recipient_zip_4_code.slice(0, 5);
        // if (obj.zip_code == parseInt(tempStr)){
            
        //     totalAward += parseFloat(obj2.total_obligated_amount);
           
        // }
        const newObj = zipCodes.filter(item => item.state == tempStr);
        let totalSum = newObj.reduce(function (result, item){
            return result + item.award;
           }, 0);
        totalAward += totalSum;

    }
    obj.award = totalAward

}

let jsonContent = JSON.stringify(zipCodes);

fs.writeFileSync('CountyAndAward2.json', jsonContent);

let newJSON = [];
const states = [ 'AL', 'AK', 'AS', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'DC', 'FM', 'FL', 'GA', 'GU', 'HI', 'ID', 'IL', 'IN', 
'IA', 'KS', 'KY', 'LA', 'ME', 'MH', 'MD', 'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ', 'NM', 'NY', 'NC', 'ND', 
'MP', 'OH', 'OK', 'OR', 'PW', 'PA', 'PR', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VT', 'VI', 'VA', 'WA', 'WV', 'WI', 'WY' ];

console.log(zipCodes);
for (let i = 0; i < states.length; i++)
{
   const newObj = zipCodes.filter(item => item.state == states[i]);
//    console.log(newObj);
   let totalSum = newObj.reduce(function (result, item){
    return result + item.award;
   }, 0);
   const finalObj = {};
   finalObj.state = newObj[0].state;
   finalObj.totalStateAward = totalSum;
   newJSON.push(finalObj);
}

let newJsonFile = JSON.stringify(newJSON);

// fs.writeFileSync('AwardByState.json', newJsonFile);
