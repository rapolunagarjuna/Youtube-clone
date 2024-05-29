class UploadController {
    constructor(s3Service) {
        this.s3Service = s3Service;
    }

    async getUsers(req, res) {
        try {
            const data = await this.s3Service.uploadFile('app.js', './app.js');
            console.log('File uploaded successfully. File location:', data.Location);
            res.send('Users');
        } catch (err) {
            console.log('Error uploading file:', err);
            res.status(500).send('Error uploading file');
        }
    }
}

module.exports = UploadController;
