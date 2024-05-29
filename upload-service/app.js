require('dotenv').config();
const express = require('express');
const uploadRouter = require('./routes/UploadRoutes');

const app = express(); 
const PORT = 3000; 

app.get('/', (req, res) => {res.send('Hello World!')});

app.use("/upload", uploadRouter);


app.listen(PORT, (error) =>{ 
	if(!error) 
		console.log("Server is Successfully Running, and App is listening on port "+ PORT) 
	else
		console.log("Error occurred, server can't start", error); 
	} 
); 
