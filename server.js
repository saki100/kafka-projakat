const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const bodyParser = require('body-parser');
const cors = require('cors');
const { Server } = require('socket.io'); // Dodajte Server iz socket.io
const kafka = require('kafka-node');

const app = express();
const port = 8000;

app.use(cors());
app.use(bodyParser.json());

// HTTP server
const server = http.createServer(app);

// WebSocket server
const io = new Server(server, {
  cors: {
    origin: 'http://localhost:3000', // url reacta 
    methods: ['GET', 'POST'],
  },
});

// Kreiranje Kafka producera
const kafkaClient = new kafka.KafkaClient({ kafkaHost: 'localhost:9092' }); 
const producer = new kafka.Producer(kafkaClient);

// Array za čuvanje konektovanih klijenata
const clients = new Set();

// Endpoint za primanje podataka od Kafka consumera
app.post('/api/receive-data', (req, res) => {
    const receivedData = req.body;

    // Slanje podataka svim klijentima putem WebSocket-a
    const jsonData = JSON.stringify(receivedData);
    clients.forEach(client => {
        client.send(jsonData);
    });

    // Send a response
    res.status(200).send('Data received successfully');
});

// Endpoint za slanje zahteva
app.post('/send-request', (req, res) => {
    const selectedCity = req.body.city;

    // Slanje poruke na Kafka topic1
    const payload = [
        {
            topic: 'topic1',
            messages: [selectedCity],
        },
    ];

    producer.send(payload, (err, data) => {
        if (err) {
            console.error('Greška pri slanju poruke na Kafka:', err);
            res.status(500).json({ message: `Greška pri slanju poruke u topic1: ${err.message}` });
        } else {
            console.log('Poruka uspešno poslata na Kafka:', data);
            res.status(200).json({ message: `Grad ${selectedCity} uspešno poslat u topic1 i obradjen!` });
        }
    });
});





// WebSocket handler
io.on('connection', (socket) => {
    console.log('New WebSocket connection');

    // Dodaj novog klijenta u listu
    clients.add(socket);

    // Kada klijent zatvori vezu, ukloni ga iz liste
    socket.on('disconnect', () => {
        console.log('WebSocket connection closed');
        clients.delete(socket);
    });
});

// Start the server
server.listen(port, () => {
    console.log(`Server is running on http://localhost:${port}`);
});
