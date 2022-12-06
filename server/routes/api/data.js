const express = require('express');
const mongodb = require('mongodb');


const router = express.Router();

//Get Data
router.get('/', async (req, res) =>{
    const data = await loadData();
    res.send(await data.find({}).toArray());
})


async function loadData(){
    const client = await mongodb.MongoClient.connect
    ("mongodb+srv://brookegodinez:v66XZ2Ng-xQbbBZ@cluster0.drp4k4d.mongodb.net/?retryWrites=true&w=majority", {
        useNewUrlParser: true
    });

    return client.db('Cluster0').collection('data')
;}



module.exports = router;


