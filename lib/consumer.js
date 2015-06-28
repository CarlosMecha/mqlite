
var async = require('async');
var util = require('util');

var Client = require('./client');
var Message = require('./message');

/**
 * Messages consumer.
 */
function Consumer(connection) {
    Client.call(this, connection);
}

util.inherits(Consumer, Client);

Consumer.prototype._emitResults = function(callback) {
    var self = this;
    return function(err, results) {
        if(err) {
            self.emit('error', err);
            callback(err);
        } else {
            var messages = [];
            results.forEach(function(res) {
                var message = new Message({
                    topic: res.topic,
                    timestamp: res.timestamp,
                    uuid: res.uuid,
                    format: res.format
                }, res.payload);
                self.emit('message', message);
                messages.push(message);
            });
            callback(null, messages);
        }

    };
};

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
    this.channel.get(topic, limit, true, this._emitResults(callback));
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
    this.channel.get(topic, limit, false, this._emitResults(callback));
};

module.exports = Consumer;

