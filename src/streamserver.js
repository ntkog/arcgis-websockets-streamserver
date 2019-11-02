const setup = require('./lib/setup.js');
// process tunning
process.setMaxListeners(0);

async function start(conf){

  // Check if source ws is alive and take first payload
  let {wsClient,wsPayload} = await setup.ws.alive(conf.wsUrl)
    .catch((err) => {
      throw new Error(err);
    });

  // Dynamically patch StreamServer definition service
  let patchedConf = setup.streamServer.setup({
    service : conf,
    client  : wsClient,
    payload : wsPayload
  });

  setup.streamServer.start(patchedConf);
}

module.exports = {
  start: start
};
