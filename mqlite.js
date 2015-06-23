
var async = require('async');
var util = require('util');

var Connection = require('./lib/connection');

function Message(headers, payload) {
    this._rawPayload = null;
    this._headers = {
        timestamp: new Date()
    };
    this._customHeaders = headers;
    this._payload = payload;
}

function Client(connection){
    this._connection = connection;
    events.EventEmitter.call(this);
}

Client.prototype.close = function(){
};

function Producer(connection) {
    Client.call(this);
}

Producer.prototype.publish = function(message) {

};

util.inherits(Producer, Client);

module.exports = {
    Connection: Connection,
    Message: Message,
    Producer: Producer
};
 
