import { handler } from './index.js';  // Adjust the path if necessary

// Example event
const event = {
    "from": "2024-08-01 00:00:00",
    "to": "2024-08-31 23:59:59",
    "timestamp": "1722240595",
    "agencyId": 37
};

handler(event).then(response => {
    const bufferSize = response.body.length;  // Returns the size in bytes
    console.log(`Buffer size: ${bufferSize} bytes`);

    const bufferSizeInKB = response.body.length / 1024;
    console.log(`Buffer size: ${bufferSizeInKB.toFixed(2)} KB`);
}).catch(error => {
    console.error('Error:', error);
});