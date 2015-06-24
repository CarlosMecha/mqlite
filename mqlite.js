
var async = require('async');
var util = require('util');

var Connection = require('./lib/connection');

/**
 * Message.
 */
function Message(headers, payload) {
    this._rawPayload = null;
    this._headers = {
        timestamp: new Date()
    };
    this._customHeaders = headers;
    this._payload = payload;
}

/**
 * Base prototype for connection holders.
 * 
 * The client can emit these events:
 * @event error err Something happended.
 * @event close The client has been closed.
 */
function Client(connection){
    this._connection = connection;
    if(!this._connection.channels) {
        this._connection.channels = 0;
    }
    this._connection.channels++;
    this._opened = true;
    this.logger = {
        info: function(){},
        debug: function(){},
        trace: function(){},
        warn: function(){},
        error: function(){}
    };
    events.EventEmitter.call(this);
}

/**
 * Does nothing for now.
 * 
 * @event close When the client is closed.
 */
Client.prototype.close = function(){
    var self = this;
    
    if(this._opened) {
        this._connection.channels--;
        this._opened = false;
    }
    setImmediate(function(){
        self.emit('close');
    });
};

/**
 * Messages producer.
 */
function Producer(connection) {
    Client.call(this, connection);
}

util.inherits(Producer, Client);

/**
 * Publish a single message.
 *
 * @param message Message to publish.
 * @param callback Optional, callback that accepts an error parameter and the uuid of the published message.
 *
 * @event error If something bad happens.
 * @event publish The message has been published.
 */
Producer.prototype.publish = function(message, callback) {
    var self = this;
    var callback = callback || function(err, uuid) {
        if(err) {
            self.logger.error(err, {error: err});
        } else {
            self.emit('publish', uuid);
        }
    };
    
    this._connection.push(
        message.headers.topic || '_default',
        message.headers.format || 'unknown',
        message.payload,
        function(err, uuid) {
            if(err) {
                self.emit('error', err);
                callback(err);
            } else {
                self.emit('publish', uuid);
                callback(null, uuid);
            }
        }
    );
};

/**
 * Messages consumer.
 */
function Consumer(connection) {
    Client.call(this, connection);
}

util.inherits(Consumer, Client);

/**
 * Peeks a number of messages (by default just 1).
 * 
 * @param topic Message topic.
 * @param limit Optional. Number of messages, by default 1.
 * @param callback Optional. Accepts an error parameter an results.
 *
 * @event error If something bad happens.
 * @event results Results from the operation.
 */
Consumer.prototype.peek = function(topic, limit, callback) {
    var self = this;
    var callback = callback || function(err, results) {
        if(err) {
            self.logger.error(err, {error: err});
        } else {
            self.logger.debug(results);
        }
    };
    var limit = limit || 1;
    this._connection.get(topic, limit, true, function(err, results) {
        if(err) {
            self.emit('error', err);
            callback(err);
        } else {
            self.emit('results', results);
            callback(null, results);
        }
    });
};

/**
 * Gets a number of messages (by default just 1).
 * 
 * @param topic Message topic.
 * @param limit Optional. Number of messages, by default 1.
 * @param callback Optional. Accepts an error parameter an results.
 *
 * @event error If something bad happens.
 * @event message A message.
 */
Consumer.prototype.get = function(topic, limit, callback) {
    var self = this;
    var callback = callback || function(err, results) {
        if(err) {
            self.logger.error(err, {error: err});
        } else {
            self.logger.debug(results);
        }
    };
    var limit = limit || 1;
    this._connection.get(topic, limit, false, function(err, results) {
        if(err) {
            self.emit('error', err);
            callback(err);
        } else {
            results.forEach(function(message) {
                self.emit('message', message);
            });
            callback(null, results);
        }
    });
};

module.exports = {
    Connection: Connection,
    Message: Message,
    Producer: Producer,
    Consumer: Consumer
};
 
