const { Client } = require("pg");

class timescaledb_connector {
    constructor() {
        this.client = new Client({
          user: "postgres",
          host: "localhost",
          database: "crypto_data",
          password: "{your timescaledb password}", //TODO your timescaledb password
          port: "5432"
        });
        this.client.connect();    
    }

    insert_trade(trade) {
      var query = "INSERT INTO trades(time, symbol, exchange, price, quantity, tid, " + 
      " type, buysell, ex_time_ms, utc_ms, utc_str, bid, sid) " +
      "VALUES(NOW(), " 
      + "'" + trade.s + "', " 
      + "'" + trade.e + "', "
      + trade.p + ","
      + trade.q + ","
      + "'" + trade.id + "',"
      + "'" + trade.t + "',"
      + "'" + trade.bs + "',"            
      + trade.ex_time + ","
      + trade.utc + ","
      + "'" + trade.utc_str + "',"
      + "'" + trade.bid + "',"
      + "'" + trade.sid + "');"      
      this.client.query(query)      
      .catch(e => {
        console.error(e.stack);
        this.client.end();
      })
    }    
}

module.exports = timescaledb_connector