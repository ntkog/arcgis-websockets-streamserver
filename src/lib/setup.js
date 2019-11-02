// websockets
const websocket = require('websocket-stream');

// Http server
const http = require('http');
const Router = require('router');
const finalhandler = require('finalhandler');

// Streams
const {chain}  = require('stream-chain');
const streamjson = require('stream-json');
const {parser} = require('stream-json/Parser');
const {streamValues} = require('stream-json/streamers/StreamValues');

// PipeLines
const defaultPipeline = require('./pipelines/default.js');

// Filters
const streamServerFilter = require('./utils/filter_utils.js');

// Esri Types
const esriTypes = require('./utils/esri_types.js');
const uuidv4 = require('uuid/v4');

const JSAPI_VERSION = process.argv[2] || "4.11";



function _shouldChallenge(origin) {
  return !/arcgis\.com/.test(origin) || !/^(3\.[1-9][0-9]|4\.[1-8]?)$/.test(JSAPI_VERSION);
}

function _isFilterChallenge(obj) {
    return obj.hasOwnProperty("filter"); // && obj.filter.hasOwnProperty("outFields");
}

function _properFilter(str) {
  if (str.length === 0){
    return false;
  } else {
    try {
      let items = str.split(" ");
      console.log(`Filter Items: (${items})`);
      return true;
    } catch(err) {
      console.error(`Failed parsing filter`);
      return false;
    }
  }
}


function _setupSource(obj) {
  //console.log( `WS Server ready at [${conf.ws.client.wsUrl}/${BASE_URL}/subscribe]`);
  return function handle(stream, request) {
    console.log(request.headers.origin);
    stream.binary = false;
    stream.socket.uuid = uuidv4();
    stream.socket.challenge = _shouldChallenge(request.headers.origin);
    console.log(`client [${stream.socket.uuid}] connected`);

    var pipeline = chain([
      ...defaultPipeline({ geo : obj.geo, service : obj.service}),
      data => {
        return stream.socket.challenge ? data : null
      },
      data => {

        return stream.socket.hasOwnProperty("filter")
          ? streamServerFilter(data.value.attributes,stream.socket.filter)
            ? data
            : null
          : data;
      },
      data => JSON.stringify(data.value)
    ]);

    pipeline.on("error", function(err){
      console.error(err);
    })

    obj.pullStream
      .pipe(pipeline)
      .pipe(stream)


    stream.on('data', function(buf){
      let data = buf.toString();
      console.log(`${data} from [${stream.socket.uuid}]`);
      try {
        let clientMessage = JSON.parse(data);
        if(_shouldChallenge(request.headers.origin) && _isFilterChallenge(clientMessage)){
          console.log(`Received challenge filter from [${stream.socket.uuid}]`);
          // Challenge
          stream.write(JSON.stringify({
            error: null,
            ...clientMessage
          }));
          console.log(`Challenge done for [${stream.socket.uuid}]`);

        }

        if (clientMessage.filter.hasOwnProperty("where") && _properFilter(clientMessage.filter.where)) {
          // Store filter for websocket client

          if(/custom_reset/.test(clientMessage.filter.where)) {
            delete stream.socket.filter;
          } else {
            console.log(`Applied filter : [${clientMessage.filter.where}]`);

            stream.socket.filter = clientMessage.filter.where;
            console.log(stream.socket.filter);
          }

        }

      } catch(err) {
        console.log(`bad payload[${data}]`);
      }

    });
    stream.on('close',function(){
      console.log(`client [${stream.socket.uuid}] disconnected`);
    });

    stream.on('error', function(err){
      console.log(`Abrupted disconnection from [${stream.socket.uuid}]`);
      disconnectPipeline();
    });

    function disconnectPipeline() {
      if(pipeline) {
        pipeline.unpipe();
        pipeline.destroy();
      }
    }
  }
}

function _setupHTTPServer(serviceConf){
  var router = Router();
  let isNgrok = /ngrok.io/.test(serviceConf.host);
  let wsServerPort = isNgrok
      ? ""
      : serviceConf.port
        ? `:${serviceConf.port}`
        : "";
  let wsServerUrl = `${isNgrok ? "wss" : "ws"}://${serviceConf.host}${wsServerPort}${serviceConf.base_url}`;
  // Update serviceConf.info prior to serve it from HTTPServer
  serviceConf.info.streamUrls = [{
    transport : "ws",
    urls: [
      //`wss://${wsUrl}`,
      `${wsServerUrl}`
    ]
  }];

  router.get(`${serviceConf.base_url}`, function (req, res) {
    res.setHeader('Content-Type', 'application/json; charset=utf-8');
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
    res.write(JSON.stringify(serviceConf.info));
    res.statusCode = 200;
    res.end();

  });

  // Retro-compatibility end-point (versions before 4.8)
  router.get(`/arcgis/rest/info`, function (req, res) {
    res.setHeader('Content-Type', 'application/json; charset=utf-8');
    res.setHeader("Access-Control-Allow-Origin", "*");
    res.setHeader("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
    res.write(JSON.stringify({
          currentVersion: 10.5,
          fullVersion: "10.5.0",
          authInfo: {
              isTokenBasedSecurity: false
          }
    }));
    res.statusCode = 200;
    res.end();
  });

  let server = http.createServer(function(req, res) {
    router(req, res, finalhandler(req, res));
  });

  server.listen(serviceConf.port, function() {
    console.log(`Your StreamServer is ready on [${serviceConf.protocol}://${serviceConf.host}${wsServerPort}${serviceConf.base_url}]`);
  });

  return server;
}

function _setupStreamServerInfo(config) {
  let fields = esriTypes.convertToEsriFields(config.payload);
  let newConfig = {
    ws : {
      server : {
        port : config.service.port,
        protocol : "ws",
        host : config.service.host,
      },
      client : {...config.client}
    },
    service : {...config.service,
      info : _updateServiceInfo({ fields : fields}),
      base_url : `/arcgis/rest/services/${config.service.name}/StreamServer`
    }
  };

  return newConfig;
}


function _updateServiceInfo(obj) {
  let service = require('../templates/service.json');
  return {...service,...obj}
}

function _guessGeoFields (arr) {
  let latFieldsNames = ["latitude","coordenadas_y","lat","y"];
  let lonFieldsNames = ["longitude","coordenadas_x","long","lon","lng","x"];
  let regexLat = new RegExp(`\\b(${latFieldsNames.join("|")})\\b`, "i");
  let regexLon = new RegExp(`\\b(${lonFieldsNames.join("|")})\\b`, "i");
  // Lat & and Lon has to be float numbers
  let candidates = arr
    .filter(fieldObj => fieldObj.type === "esriFieldTypeDouble");

  let names = candidates.map(e => e.name);
  let latField = names.find(name => regexLat.test(name));
  let lonField = names.find(name => regexLon.test(name));
  // TO BE Reviewed

  if (latField === null || lonField === null) {
    console.warn("Unable to find field candidates on payload. Skipping Re-Projecction");
    return null;
  } else {
    console.log(`Found spatial information in fields [${lonField},${latField}]`);
    return {
      lat : latField,
      lon : lonField
    };
  }
}


function _wsClientAlive (wsUrl) {
  return new Promise((resolve, reject) => {
    try {
      let {hostname,port,protocol} = new URL(wsUrl);
      let ws = websocket(wsUrl);
      const pipemod = chain([
        parser({packValues: true}),
        streamValues(),
        data => {
          ws.unpipe();
          resolve({
            wsPayload : data.value,
            wsClient : {
              host : hostname,
              port : port,
              protocol : protocol,
              wsUrl : wsUrl
            }
          });
        }
      ]);

      ws.on("error", function(err){
        reject(`Cannot connect to [${wsUrl}]`);
      })

      pipemod
        .on("error", function(err){
          reject(err);
        });

      ws.pipe(pipemod);
    } catch(err) {
      reject(err);
    }

  })
}

function _wsClient (wsUrl) {
  let wsClient = websocket(wsUrl, {
    perMessageDeflate: false
  });
  wsClient.setMaxListeners(0);
  return wsClient;
}

module.exports = {
  ws : {
    alive : _wsClientAlive
  },
  streamServer : {
    setup : _setupStreamServerInfo,
    start : (conf) => {
      // Now some Plumbing...
      let HTTPServer = _setupHTTPServer(conf.service);
      let wsRemoteClient = _wsClient(conf.ws.client.wsUrl);
      let ws_server = websocket.createServer({
         server : HTTPServer,
         path : `${conf.service.base_url}/subscribe`,
         binary : false
      },_setupSource({
          pullStream : wsRemoteClient,
          service: conf.service,
          geo : _guessGeoFields(conf.service.info.fields)
        })
      );
      ws_server.on("error", function(err){
        console.log( `CACHED on WS [${err}]`);
      });
    }
  }
}
