const amqp = require('amqplib');
const WebSocket = require('ws');

// Configuración de RabbitMQ
const amqpUrl = 'amqp://44.208.120.29';
const queue = 'mqtt';

// Configuración de WebSocket
const wss = new WebSocket.Server({ port: 8080 });

// Función para enviar datos a todos los clientes WebSocket conectados
const broadcastData = (data) => {
  wss.clients.forEach(client => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(JSON.stringify(data));
    }
  });
};

// Conectar a RabbitMQ y consumir mensajes
const startConsumer = async () => {
  try {
    const connection = await amqp.connect(amqpUrl);
    const channel = await connection.createChannel();

    await channel.assertQueue(queue, {
      durable: true
    });

    console.log('Waiting for messages in %s. To exit press CTRL+C', queue);

    channel.consume(queue, (msg) => {
      if (msg !== null) {
        try {
          console.log('Received:', msg.content.toString());
          const data = JSON.parse(msg.content.toString());

          // Validar datos recibidos
          if (data.temperature != null && data.soilMoisture != null && data.waterLevel != null) {
            console.log('Data is valid:', data);

            // Enviar los datos a los clientes WebSocket
            broadcastData(data);
          } else {
            console.error('Invalid data format:', data);
          }

          // Confirmar recepción del mensaje
          channel.ack(msg);
        } catch (err) {
          console.error('Error processing message:', err);
          console.error('Message content:', msg.content.toString());
          // No confirmar el mensaje para que pueda ser reintentado
        }
      }
    });
  } catch (error) {
    console.error('Error in RabbitMQ consumer:', error);
  }
};

// Iniciar el consumidor de RabbitMQ
startConsumer();
