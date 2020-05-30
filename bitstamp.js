const MongoClient = require('mongodb').MongoClient;
const timescaledb_connector = require('./timescaledb');

class bitstamp_market_data {
    constructor(symbol) {
        this.exchange = 'bitstamp';
        this.symbol = symbol;        
        this.url = 'wss://ws.bitstamp.net';                
        this.timescaledb_connector = new timescaledb_connector();
        this.db_user = "{your mongodb username}";
        this.db_pw = "{your mongodb password}";
        this.db_name = "{your mongodb name}";
        this.uri = "{your mongodb connection string}";  
    }
    
    reset() {
    }

    get_timestamp() {
        return new Date(Date.now()).toISOString();
    }
    
    write_to_db(trade) {    
        var insertDocuments = function(trade, client, callback) {
            var collection = client.db(this.db_name).collection('trades');
            collection.insertMany([
              trade
            ], function(err, result) {                            
                callback(result);
            });
          }

        MongoClient.connect(this.uri, { useNewUrlParser: true, useUnifiedTopology: true }, function(err, client) {            
            insertDocuments(trade, client, function() {
                client.close();
            });
        });        
    }

    parse_trade(data) {        
        var now = Date.now();
        var now_str = new Date(now).toISOString();

        var trade = {
            // ex_time: parseFloat(data.timestamp),
            // ex_time_micro: parseFloat(data.microtimestamp),
            ex_time: parseFloat(data.microtimestamp) / 1000, //become millseconds, align with Gemini.
            s: this.symbol,
            e: this.exchange,            
            p: data.price,
            q: data.amount,            
            id: data.id,
            t: 'trade',            
            bs: data.type, //0:buy, 1:sell
            utc: now,
            utc_str: now_str,
            bid: data.buy_order_id,
            sid: data.sell_order_id                        
        };
        // this.write_to_db(trade);        
        console.log(JSON.stringify(trade));        
        // this.timescaledb_connector.insert_trade(trade);
    }
    
    get_subscribe_msg() {
        var subscribeMsg = {
            "event": "bts:subscribe",
            "data": {
                "channel": "live_trades_" + this.symbol
            }
        };
        return JSON.stringify(subscribeMsg);
    }

    on_recv_msg(event) {        
        var response = JSON.parse(event.data);
        switch(response.event) {
            case "trade":
                this.parse_trade(response.data);
                break;
            case "bts:request_reconnect":
                console.error(this.exchange + ' - ' + this.symbol + ' do reconnect please');
                break;
        }    
    };
}

module.exports = bitstamp_market_data