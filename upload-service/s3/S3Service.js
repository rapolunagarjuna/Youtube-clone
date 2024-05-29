const AWS = require('aws-sdk');
const fs = require('fs');

class S3Service {
    constructor(region, accessKeyId, secretAccessKey, bucketName) {
        this.s3 = new AWS.S3({
            region: region,
            accessKeyId: accessKeyId,
            secretAccessKey: secretAccessKey
        });
        this.bucketName = bucketName;
    }

    uploadFile(key, filePath) {
        const params = {
            Bucket: this.bucketName,
            Key: key,
            Body: fs.createReadStream(filePath)
        };
        return this.s3.upload(params).promise();
    }
}

module.exports = S3Service;
