const streamServer = require('arcgis-websockets-streamserver');

const SERVICE_CONF = {
  name : "twitter",
  out_sr : {
    wkid : 102100,
    latestWkid : 3857
  },
  port : process.env["NGROK"] ? 9000 : 9000,
  host : process.env["NGROK"]  || "localhost",
  protocol : process.env["NGROK"] ? "https" : "http",
  wsUrl : "ws://localhost:8000"
};

// process
//   .on('unhandledRejection', (reason, p) => {
//     console.error(reason, 'Unhandled Rejection at Promise', p);
//   })
//   .on('uncaughtException', err => {
//     console.error(err, 'Uncaught Exception thrown');
//   });



streamServer.start(SERVICE_CONF)
  .then(() => console.log("Initialized successfully"))
  .catch((err) => {
    console.log(`Initialization failed! reason : [${err}]`);
    process.exit(12);
  });
