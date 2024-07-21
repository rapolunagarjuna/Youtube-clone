const multer = require('multer');
const { multipartUpload } = require('../s3/S3Service');
const kafka = require('kafka-node');

const upload = multer({ dest: 'uploads/' });
const kafkaHost = process.env.KAFKA_BROKER || 'localhost:9092';
// Kafka Producer setup
const client = new kafka.KafkaClient({ kafkaHost: kafkaHost });
const producer = new kafka.Producer(client);
console.log(`Attempting to connect to Kafka at ${kafkaHost}`);

producer.on('ready', () => {
    console.log('Kafka Producer is connected and ready.');
});

producer.on('error', (error) => {
    console.error('Error in Kafka Producer', error);
});


const uploadVideo = async (req, res) => {
    try {
        const filePath = req.file.path;
        const fileName = req.file.originalname;

        const result = await multipartUpload(filePath, fileName);

        // Publish message to Kafka
        const payloads = [
            {
                topic: 'video-uploads',
                messages: JSON.stringify({ fileName, s3Location: result.Location }),
            },
        ];
        producer.send(payloads, (err, data) => {
            if (err) {
                console.error('Failed to send message to Kafka', err);
            } else {
                console.log('Message sent to Kafka', data);
            }
        });

        res.status(200).json({
            message: 'Video uploaded successfully',
        });
    } catch (error) {
        res.status(500).json({
            message: 'Failed to upload video',
            error: error.message,
        });
    }
};

module.exports = { upload, uploadVideo };
