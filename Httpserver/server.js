var http = require('http');
var request_counter =0;
var response_counter =0;
const server = http.createServer(function (req, res) {
    if (req.url === '/ping' && req.method === 'GET') {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.write('ping localhost 8080');
        res.end();
    }

    else if (req.url === '/metrics' && req.method === 'GET') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        const metrics = {
            requests: request_counter,
            responses: response_counter
        };
        res.write(JSON.stringify(metrics));
        res.end();
    }

    else {
        res.writeHead(200, { 'Content-Type': 'text/plain' });
        res.write('Hello World');
        res.end();
    }
});


server.on('request', () => {
    request_counter++;
});

server.on('close', () => {
    console.log('Server is closing...');
});

server.listen((8080),()=>{
    console.log('Server is running at port 8080');
})