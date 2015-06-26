
var util = require('util');

var Client = require('./client');

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

module.exports = Producer;

