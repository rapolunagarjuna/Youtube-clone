const AWS = require('aws-sdk');
const ffmpeg = require('fluent-ffmpeg');
const ffmpegInstaller = require('@ffmpeg-installer/ffmpeg');
const fs = require('fs');
const path = require('path');
const dotenv = require('dotenv');
const kafka = require('kafka-node');

// Load environment variables from .env file
dotenv.config();

// Set FFmpeg path
ffmpeg.setFfmpegPath(ffmpegInstaller.path);

// Configure AWS SDK
AWS.config.update({
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    region: process.env.AWS_REGION,
});

const s3 = new AWS.S3();

const downloadFromS3 = async (bucket, key, downloadPath) => {
    const params = { Bucket: bucket, Key: key };
    const file = fs.createWriteStream(downloadPath);
    return new Promise((resolve, reject) => {
        s3.getObject(params)
            .createReadStream()
            .pipe(file)
            .on('finish', resolve)
            .on('error', reject);
    });
};

const uploadToS3 = async (bucket, key, filePath) => {
    const fileContent = fs.createReadStream(filePath);
    const params = {
        Bucket: bucket,
        Key: key,
        Body: fileContent,
    };
    return s3.upload(params).promise();
};

const transcodeToHLS = (inputPath, resolution) => {
    return new Promise((resolve, reject) => {
        const outputFileName = path.basename(inputPath, path.extname(inputPath));
        const outputPath = path.join(__dirname, 'transcoded', `${outputFileName}_${resolution}.m3u8`);
        
        ffmpeg(inputPath)
            .addOptions([
                `-profile:v baseline`,
                `-level 3.0`,
                `-start_number 0`,
                `-hls_time 10`,
                '-hls_playlist_type vod',
                `-vf scale=${resolution}`,
                `-hls_list_size 0`,
                `-f hls`,
            ])
            .output(outputPath)
            .on('end', () => {
                console.log(`Transcoding finished for resolution ${resolution}`);
                resolve(outputPath);
            })
            .on('error', (err) => {
                console.error(`Error transcoding for resolution ${resolution}: ${err.message}`);
                reject(err);
            })
            .run();
    });
};

const processVideo = async (bucket, s3Location) => {
    const inputFileName = decodeURIComponent(new URL(s3Location).pathname.substring(1));
    const inputPath = path.join(__dirname, 'downloads', path.basename(inputFileName)); 
    // Download the video from S3
    console.log(`Downloading ${inputFileName} to ${inputPath}`);
    await downloadFromS3(bucket, inputFileName, inputPath);
    console.log(`Downloaded ${inputFileName} to ${inputPath}`);
    
    // Transcode the video to different resolutions
    const resolutions = ["1920x1080", "1280x720", "854x480", "640x360", "426x240"];
    const promises = resolutions.map(resolution => transcodeToHLS(inputPath, resolution));

    try {
        const transcodedPaths = await Promise.all(promises);

        // Upload the transcoded files to S3
        const uploadPromises = transcodedPaths.map(outputPath => {
            const key = `${inputFileName}/${path.basename(outputPath)}`;
            return uploadToS3(bucket, key, outputPath);
        });

        await Promise.all(uploadPromises);
        console.log('All files uploaded successfully.');

    } catch (error) {
        console.error('Error during processing:', error);
    } finally {
        // Clean up local files
        fs.unlinkSync(inputPath);
        resolutions.forEach(resolution => {
            const outputFileName = path.basename(inputPath, path.extname(inputPath));
            const m3u8Path = path.join(__dirname, 'transcoded', `${outputFileName}_${resolution}.m3u8`);
            if (fs.existsSync(m3u8Path)) fs.unlinkSync(m3u8Path);

            // Delete .ts files
            const tsFiles = fs.readdirSync(path.join(__dirname, 'transcoded'))
                              .filter(file => file.startsWith(`${outputFileName}_${resolution}`) && file.endsWith('.ts'));
            tsFiles.forEach(tsFile => fs.unlinkSync(path.join(__dirname, 'transcoded', tsFile)));
        });
    }
};

const kafkaHost = process.env.KAFKA_BROKER || 'localhost:9092';
const client = new kafka.KafkaClient({ kafkaHost: kafkaHost });
const consumer = new kafka.Consumer(
    client,
    [
        { topic: 'video-uploads', partition: 0 },
    ],
    {
        autoCommit: true, // Enable auto commit
        groupId: 'video-processor-group', // Use a group id for consumer group
        fromOffset: 'latest' // Start reading new messages
    }
);

consumer.on('message', async function (message) {
    const bucket = process.env.S3_BUCKET_NAME;
    const parsedMessage = JSON.parse(message.value);
    const { s3Location } = parsedMessage;
    console.log(`Message received, Processing video from ${s3Location}`);
    await processVideo(bucket, s3Location);
});

consumer.on('error', function (err) {
    console.error('Error in Kafka consumer:', err);
});

consumer.on('offsetOutOfRange', function (err) {
    console.error('Offset out of range error in Kafka consumer:', err);
});