const MongoClient = require('mongodb').MongoClient;
const timescaledb_connector = require('./timescaledb');

class gemini_market_data {
    constructor(symbol) {
        this.exchange = 'gemini';
        this.symbol = symbol;        
        this.curr_socket_sequence = -1; //init
        this.url = 'wss://api.gemini.com/v1/marketdata/' + symbol + '?top_of_book=true&trades=true&auctions=true&heartbeat=true';        
        this.timescaledb_connector = new timescaledb_connector();        
        this.db_user = "{your mongodb username}";
        this.db_pw = "{your mongodb password}";
        this.db_name = "{your mongodb name}";
        this.uri = "{your mongodb connection string}";        
    }
    
    reset() {
        this.curr_socket_sequence = -1; //init
    }

    get_subscribe_msg() {
        return '';
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
        var events = data['events'];
        var length = events.length;            
        var i;
        for (i=0; i<length; i++) {
            var x = JSON.stringify(events[i]);
            var y = JSON.parse(x);
            var t = y['type']; //{trade, auction_open}
            switch (t) {                
                case 'trade':
                    var trade = {                
                        ex_time: data['timestampms'], //milliseconds
                        s: this.symbol,
                        e: this.exchange,
                        p: parseFloat(y['price']),
                        q: parseFloat(y['amount']),                
                        id: y['tid'],
                        t: t, 
                        bs: y['makerSide'], //{bid, ask, auction}          
                        utc: now,
                        utc_str: now_str
                    };
                    // this.write_to_db(trade);
                    console.log(JSON.stringify(trade));
                    // this.timescaledb_connector.insert_trade(trade);
                    break;
                case 'block_trade':
                    var block_trade = {                
                        ex_time: data['timestampms'], //milliseconds
                        s: this.symbol,
                        e: this.exchange,
                        p: parseFloat(y['price']),
                        q: parseFloat(y['amount']),                
                        id: y['tid'],
                        t: t, 
                        // bs: y['makerSide'], //{bid, ask}          
                        utc: now,
                        utc_str: now_str
                    };
                    console.log(JSON.stringify(block_trade));
                    // this.timescaledb_connector.insert_trade(block_trade);
                    break;                    
                default:
                    console.log('what? ' + JSON.stringify(data));
                    break;
            }
        }
    }
    
    on_recv_msg(event) {        
        var data = JSON.parse(event.data);
        // console.log('data=' + JSON.stringify(data));
        var type = data['type'];
        var socket_sequence = data['socket_sequence'];
        if (socket_sequence == this.curr_socket_sequence + 1) {
            this.curr_socket_sequence = this.curr_socket_sequence + 1
        } else {
            console.error(this.get_timestamp() + " - " + this.symbol + " seq num missing. curr=" + this.curr_socket_sequence + ", recv=" + socket_sequence);
        }
        switch(type) {
            case "update":
                    this.parse_trade(data);
                break;
            case "heartbeat":
                console.log(this.get_timestamp() + " - " + this.exchange + '|'+ this.symbol + " heartbeat, seq=" + socket_sequence);
                break;
            default:
                console.log(this.exchange + '|' + this.symbol + ' recv ' + type);
                break;
        }    
    };
}

module.exports = gemini_market_data