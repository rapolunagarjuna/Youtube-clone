require('dotenv').config();
const express = require('express');
const AWS = require('aws-sdk');
const kafka = require('kafka-node');
const retry = require('retry');
const uploadRouter = require('./routes/UploadRoutes');

const app = express(); 
const PORT = 3000; 
app.use(express.json());

app.get('/', (req, res) => { res.send('Hello World!') });
app.use("/videos", uploadRouter);

const kafkaHost = process.env.KAFKA_BROKER || 'localhost:9092';

// Retry mechanism for Kafka connection
const connectToKafka = (callback) => {
    const operation = retry.operation({
        retries: 5,
        factor: 2,
        minTimeout: 1000,
        maxTimeout: 5000,
    });

    operation.attempt((currentAttempt) => {
        console.log(`Connecting to Kafka (attempt ${currentAttempt})`);
        const client = new kafka.KafkaClient({ kafkaHost });
        const producer = new kafka.Producer(client);

        producer.on('ready', () => {
            console.log('Kafka Producer is connected and ready.');
            callback(null, producer);
        });

        producer.on('error', (error) => {
            if (operation.retry(error)) {
                console.error('Error in Kafka Producer, retrying...', error);
                return;
            }
            console.error('Error in Kafka Producer, giving up.', error);
            callback(error);
        });
    });
};

// Retry mechanism for S3 connection
const connectToS3 = (callback) => {
    const operation = retry.operation({
        retries: 5,
        factor: 2,
        minTimeout: 1000,
        maxTimeout: 5000,
    });

    operation.attempt((currentAttempt) => {
        console.log(`Connecting to S3 (attempt ${currentAttempt})`);

        const s3 = new AWS.S3({
            accessKeyId: process.env.AWS_ACCESS_KEY_ID,
            secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
            region: process.env.AWS_REGION,
        });

        s3.listBuckets((err, data) => {
            if (err) {
                if (operation.retry(err)) {
                    console.error('Error connecting to S3, retrying...', err);
                    return;
                }
                console.error('Error connecting to S3, giving up.', err);
                callback(err);
            } else {
                console.log('Connected to S3');
                callback(null, s3);
            }
        });
    });
};

const startServer = (producer, s3) => {
    app.locals.producer = producer;
    app.locals.s3 = s3;

    app.listen(PORT, (error) => {
        if (!error)
            console.log("Server is Successfully Running, and App is listening on port " + PORT);
        else
            console.log("Error occurred, server can't start", error);
    });
};

connectToKafka((kafkaErr, producer) => {
    if (kafkaErr) {
        console.error('Failed to connect to Kafka:', kafkaErr);
        process.exit(1);
    } else {
        connectToS3((s3Err, s3) => {
            if (s3Err) {
                console.error('Failed to connect to S3:', s3Err);
                process.exit(1);
            } else {
                startServer(producer, s3);
            }
        });
    }
});
