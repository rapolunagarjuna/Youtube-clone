const express = require('express');
const { upload, uploadVideo } = require('../controllers/UploadController');

const router = express.Router();

router.post('/upload', upload.single('video'), uploadVideo);

module.exports = router;
