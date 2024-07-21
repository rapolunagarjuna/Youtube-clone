const AWS = require('aws-sdk');
const s3 = new AWS.S3();
const fs = require('fs');

// Configure AWS SDK to use environment variables
AWS.config.update({
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    region: process.env.AWS_REGION,
});

const S3_BUCKET_NAME = process.env.AWS_BUCKET_NAME;
const CHUNK_SIZE = 5 * 1024 * 1024; // 5MB per chunk

const createMultipartUpload = async (fileName) => {
    const params = {
        Bucket: S3_BUCKET_NAME,
        Key: fileName,
    };
    const response = await s3.createMultipartUpload(params).promise();
    return response.UploadId;
};

const uploadPart = async (fileName, uploadId, partNumber, body) => {
    const params = {
        Bucket: S3_BUCKET_NAME,
        Key: fileName,
        PartNumber: partNumber,
        UploadId: uploadId,
        Body: body,
    };
    const response = await s3.uploadPart(params).promise();
    return response.ETag;
};

const completeMultipartUpload = async (fileName, uploadId, parts) => {
    const params = {
        Bucket: S3_BUCKET_NAME,
        Key: fileName,
        MultipartUpload: { Parts: parts },
        UploadId: uploadId,
    };
    const response = await s3.completeMultipartUpload(params).promise();
    return response;
};

const multipartUpload = async (filePath, fileName) => {
    const uploadId = await createMultipartUpload(fileName);
    const fileStream = fs.createReadStream(filePath, { highWaterMark: CHUNK_SIZE });
    const parts = [];
    let partNumber = 1;

    for await (const chunk of fileStream) {
        const eTag = await uploadPart(fileName, uploadId, partNumber, chunk);
        parts.push({ ETag: eTag, PartNumber: partNumber });
        partNumber++;
    }

    return await completeMultipartUpload(fileName, uploadId, parts);
};

module.exports = { multipartUpload };
