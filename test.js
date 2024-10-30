import { handler } from './index.js';  // Adjust the path if necessary

// Example event
const event = {
    "from": "2024-10-01 00:00:00",
    "to": "2024-10-31 23:59:59",
    "timestamp": "1730270453",
    "agencyId": 38,
    "key": "38-isbsf-2024-10-01::2024-10-31",
    "jobId": "99db3675-1505-408b-bbec-283ed291a11a",
};

console.time('s');
handler(event).then(response => {
    const bufferSize = response.body.length;  // Returns the size in bytes
    console.log(`Buffer size: ${bufferSize} bytes`);

    const bufferSizeInKB = response.body.length / 1024;
    console.log(`Buffer size: ${bufferSizeInKB.toFixed(2)} KB`);
}).catch(error => {
    console.error('Error:', error);
}).finally(() => {
    console.timeEnd('s');
});
