const express = require('express');
const logger = require('morgan');
const path = require('path');
const bodyParser = require('body-parser');
const ptl = require('./app/api/ptl');
const db = require('./app/helpers/pg.js');


const app = express();

app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: false }));

app.use(express.static(path.join(__dirname, 'public')));

app.get('/test', (req, res) => {
    res.send('Welcome to your express API');
});

app.get('/api/ptl', ptl.getDbInfo);
app.get('/api/ptl/detail/:id', ptl.getColInfo);
app.get('/api/ptl/content/:tbname', ptl.getContent);

app.listen(5000, () => console.log('App running on port 5000 🔥'));
