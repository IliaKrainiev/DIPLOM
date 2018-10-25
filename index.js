var fs = require('fs');
var CsvReadableStream = require('csv-reader');
var AutoDetectDecoderStream = require('autodetect-decoder-stream');
const EventEmitter = require('events');

var inputStream = fs.createReadStream('/home/ilia/RoundRobin/data/brainhead_train.csv')
    .pipe(new AutoDetectDecoderStream({defaultEncoding: '1255'})); // If failed to guess encoding, default to 1255

// The AutoDetectDecoderStream will know if the stream is UTF8, windows-1255, windows-1252 etc.
// It will pass a properly decoded data to the CsvReader.

var currentServerNumber = 1;
var allData = [];
var myEmitter = new EventEmitter();
myEmitter.on('add', (n) => {
    allData[n][4] = +1;
});
var processedRequests = 0;

myEmitter.on('add processed', () => {
    processedRequests = processedRequests + 1;
    console.log('processedRequests >> > ', processedRequests);
});
myEmitter.on('remove', (n) => {
    allData[n][4] = -1;
});

myEmitter.on('change server', () => {
    if (currentServerNumber === allData.length - 1) {
        currentServerNumber = 1;
    } else {
        currentServerNumber = currentServerNumber + 1;
    }
});

inputStream
    .pipe(CsvReadableStream({parseNumbers: true, parseBooleans: true, trim: true}))
    .on('data', function (row) {
        allData.push(row);
    }).on('end', function (data) {
    console.log('No more rows!');
});


setTimeout(() => {
    countData(allData);
    setInterval(() => console.log('processedRequests   >>> >', processedRequests), 1000)
}, 2000);


function countData(allData) {
    var timesArr = [];
    for (let i = 0; i < 100; i++) {
        timesArr.push([(-0.25 * Math.log(1 - Math.random())),
            (-0.333 * Math.log(1 - Math.random()))])
    }
    setTimeoutsForRequests(timesArr, allData).then(() => console.log('Haos start > > > >'));
}

function setTimeoutsForRequests(timesArr) {
    var timeouts = [];
    for (let el of timesArr) {
        timeouts.push(new Promise((res) => {
            res(setTimeout(() => {
                RoundRobin(el[1])
            }, el[0]))
        }));
    }

    return Promise.all(timeouts)
}

function RoundRobin(taskTime) {
    //Fix all servers
    for (let o = 0; o < allData.length; o++) {
        allData[o][5] = 0;
    }

    //Break random number of servers
    for (let o = 0; o < Math.floor(Math.random() * allData.length); o++) {
        console.log('here ');
        allData[Math.floor(Math.random() * allData.length)][5] = 1
    }

   console.log(allData);
    console.log(currentServerNumber);

    if (!allData[currentServerNumber][5] && allData[currentServerNumber][4] < allData[currentServerNumber][3]) {
        myEmitter.emit('add processed');
        myEmitter.emit('add', currentServerNumber);
        let currentServerNumberPointer = currentServerNumber;
        setTimeout(() => {
            myEmitter.emit('remove', currentServerNumberPointer);
        }, taskTime);
    }
    myEmitter.emit('change server');

}


function finish() {
    console.log('Thats all   > >>  ');
    return processedRequests;
}