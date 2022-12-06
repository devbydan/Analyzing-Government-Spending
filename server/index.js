const express = require('express');
const cors = require('cors');
const bodyParser = require('body-parser');


const app = express();

//Middleware

app.use(bodyParser.json());

app.use(cors());


const data = require('./routes/api/data');

app.use('/api/data', data);



const port = process.env.PORT || 3012


app.listen(port, ()=> console.log(`server started on port ${port}`));


