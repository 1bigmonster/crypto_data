
const gemini_market_data = require('./gemini') 
const bitstamp_market_data = require('./bitstamp') 
const WebSocket = require('ws');

function get_timestamp() {
    return new Date(Date.now()).toISOString();
}

function start(market_data, socket) { 
    var symbol = market_data.symbol;
    var exchange = market_data.exchange;
    var label = exchange + '|'+ symbol;
    socket.onerror = function(error) {
        console.error(get_timestamp() + " - " + label + " error: " + error);
    };
    socket.onopen = function(event) {
        var subscribeMsg = market_data.get_subscribe_msg();
        if (subscribeMsg != '') {
            socket.send(subscribeMsg);
            console.log(get_timestamp() + " - " + label + " sent subscribeMsg");
        }
        console.log(get_timestamp() + " - " + label + " connected");
    };
    socket.onclose = function(event) {
        console.log(get_timestamp() + " - " + label + " disconnected");
        setTimeout(() => {
            market_data.reset();
            start(market_data, socket);
        }, (1000));   //1s.
    };
    socket.onmessage = function(event) {        
        market_data.on_recv_msg(event);
    };        
}

var myArgs = process.argv.slice(2); //skip first two.
console.log('myArgs: ', myArgs);
switch (myArgs[0]) {
    case 'gemini':
        const gemini_btcusd = new gemini_market_data('btcusd');
        start(gemini_btcusd, new WebSocket(gemini_btcusd.url));
        break;
    case 'bitstamp':
        const bitstamp_btcusd = new bitstamp_market_data('btcusd');
        start(bitstamp_btcusd, new WebSocket(bitstamp_btcusd.url));
        break;
    default:
        break;        
}