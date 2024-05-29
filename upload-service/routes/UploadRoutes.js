const express = require('express');
const S3Service = require('../s3/S3Service');
const UploadController = require('../controllers/UploadController');

const router = express.Router();
const s3Service = new S3Service(
    process.env.AWS_REGION,
    process.env.AWS_ACCESS_KEY_ID,
    process.env.AWS_ACCESS_ACCESS_KEY,
    process.env.AWS_BUCKET_NAME
);

const uploadController = new UploadController(s3Service);

router.get('/', (req, res) => uploadController.getUsers(req, res));

module.exports = router;
