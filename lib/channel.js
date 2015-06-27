
var events = require('events');
var util = require('util');

var methods = ['get', 'push'];

/**
 * Represents a channel to send an receive commands from a connection.
 * 
 * Channels help to maintain concurrent access to the queue.
 * Do not create this channels manually.
 * @param connection Connection.
 */
function _Channel(id, connection) {
    this.id = id;
    this.connection = connection;
}

for(var i in methods) {
    _Channel.prototype[methods[i]] = (function(method) {
        var privateMethod = '_' + method;
        return function() {
            var args = [];
            for(var index in arguments) {
                args.push(arguments[index]);
            }
            this._call(privateMethod, args);
        };
    })(methods[i]);
}

_Channel.prototype._call = function(method, args) {
    this.connection[method].apply(this.connection, args);
};

_Channel.prototype.close = function() {
    delete this.connection.channels[this.id];
};

module.exports = _Channel;

