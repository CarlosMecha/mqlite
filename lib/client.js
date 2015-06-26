
var events = require('events');
var util = require('util');


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

util.inherits(Client, events.EventEmitter);

/**
 * Does nothing for now.
 * 
 * @event close When the client is closed.
 */
Client.prototype.close = function(callback){

    var self = this;
    var callback = callback || function() {
        self.logger.debug('Channel closed.');
    };

    if(this._opened) {
        this._connection.channels--;
        this._opened = false;
    }
    setImmediate(function(){
        self.emit('close');
        callback();
    });
};

module.exports = Client;

