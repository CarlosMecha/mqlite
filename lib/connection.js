
var async = require('async');
var events = require('events');
var fs = require('fs');
var uuidGenerator = require('node-uuid');
var util = require('util');
var sqlite = require('sqlite3').verbose();

var _checkDbStmt = 'SELECT 1';
var _checkStmt = 'SELECT 1 FROM messages';
var _insertStmt = 'INSERT INTO messages (uuid, topic, format, timestamp, payload) VALUES (?, ?, ?, ?, ?)';
var _selectStmt = 'SELECT uuid, timestamp, format, topic, payload FROM messages ORDER BY timestamp ASC LIMIT ?';
var _deleteStmt = 'DELETE FROM messages WHERE uuid = ?';
var _schema = './schema.sql';

/**
 * MQ Service constructor.
 * @param databaseFile [Optional] Database file name.
 * @param logger [Optional] Logger.
 */
function Connection(databaseFile) {
    if(databaseFile && typeof databaseFile === 'string') {
        this._dbFile = databaseFile;
    } else {
        this._dbFile = ':memory:';
    }

    this.encoders = {};
    this.decoders = {};
    this._db = null;
    this.logger = {
        info: function(){},
        debug: function(){},
        trace: function(){},
        warn: function(){},
        error: function(){}
    };
    events.EventEmitter.call(this);
}

util.inherits(Connection, events.EventEmitter);

/**
 * Default encoder.
 */
Connection.prototype.defaultEncoder = function(obj) {
    if(typeof obj === 'undefined' || obj == null){
        return null;
    } else if(typeof obj === 'string') {
        return obj
    } else {
        return JSON.stringify(obj);
    }
};

/**
 * Default decoder.
 */
Connection.prototype.defaultDecoder = function(serializedObj) {
    if(typeof serializedObj === 'undefined' || serializedObj == null){
        return null;
    } else if(typeof serializedObj === 'string') {
        try{
            return JSON.parser(serializedObj);
        } catch(e) {
            return serializedObj;
        };
    } else {
        return serializedObj;
    }
};

/**
 * Checks if the database exists and is accesible.
 */
Connection.prototype._checkDb = function(callback) {
    this._db.get(_checkDbStmt, function(err, row){
        callback((err == undefined));
    });
};

/**
 * Checks if the schema is present.
 */
Connection.prototype._checkSchema = function(callback){
    this._db.get(_checkStmt, function(err, row){
        callback((err == undefined));
    });
};

/**
 * Creates the schema.
 */
Connection.prototype._createSchema = function(callback) {
    var self = this;
    
    async.waterfall([
        function(cb){
            fs.readFile(_schema, 'utf-8', function(err, data){
                if(err){
                    cb(err);
                } else {
                    cb(null, data);
                }
            });
        },
        function(sql, cb){
            self._db.exec(sql, cb);
        }
    ], function(err, results){
        callback(err);
    });
};

/**
 * Initializes and start listening for messages.
 * @param callback Optional. Callback, it should accept an error parameter.
 * @event listen The connection is ready.
 */
Connection.prototype.listen = function(callback){
    var self = this;
    var callback = callback || function(err) {
        if(err) {
            self.logger.error(err, {error: err});
        } else {
            self.logger.debug('Mq service listening.');
        }
    }

    async.waterfall([
        function(cb) {
            var err = null;
            try {
                self._db = new sqlite.Database(self._dbFile);
            } catch(e) {
                err = e;
            }
            setTimeout(function(){
                cb(err);
            });
        },
        function(cb){
            self._checkDb(function(exists) {
                if(!exists){
                    cb(new Error('Database can\'t be created or doesn\'t exist.'));
                } else {
                    cb(null, true);
                }
            });
        },
        function(exists, cb){
            self._checkSchema(function(exists){
                cb(null, exists);
            });
        },
        function(exists, cb){
            if(exists){
                setTimeout(function(){
                    cb(null);
                }, 0);
            } else {
                self._createSchema(cb);
            }
        },
        function(cb) {
            self._insert = self._db.prepare(_insertStmt, cb);
        },
        function(cb) {
            self._select = self._db.prepare(_selectStmt, cb);
        },
        function(cb) {
            self._delete = self._db.prepare(_deleteStmt, cb);
        }
    ], function(err){
        if(err) {
            self.emit('error', err);
            callback(err);   
        } else {
            self.emit('listen');
            callback();
        }
    });
};

/**
 * Pushes a message.
 * @param topic Message topic.
 * @param format An string defining the payload's format.
 * @param payload Message payload.
 * @param callback Callback that accepts an error has first parameter, and a uuid as a second parameter.
 */
Connection.prototype.push = function(topic, format, payload, callback){
    var uuid = uuidGenerator.v1();
    this.logger.debug('Pushing message %s to queue %s, %s as %s', uuid, topic, payload, format, {});
    var timestamp = Date.now();
    var encoder = this.encoders.hasOwnProperty(format) ? this.encoders[format] : this.defaultEncoder;
    this.logger.debug('Using encoder %s', encoder, {});
    this._insert.run(uuid, topic, format, timestamp, encoder(payload), function(err){
        callback(err, err ? null : uuid);    
    });
};

/**
 * Gets messages.
 * @param topic Message topic.
 * @param limit Number of messages.
 * @param requeue Put the messages back.
 * @param callback Function that accepts an error as a first argument and list of messages as second.
 */
Connection.prototype.get = function(topic, limit, requeue, callback){
    var self = this;

    function get(cb) {
        var rows = [];
        self._select.each(
            limit,
            function(err, row){
                if(!err) {
                    rows.push(row);   
                }
            },
            function(err, numberRows){
                self.logger.debug('Returned %d rows.', numberRows);
                cb(err, rows);
            }
        );
    }

    function decode(rows, cb) {
        var messages = [];
        async.each(
            rows,
            function(row, cb) {
                self.logger.debug('Decoding %s as %s', row.payload, row.format, {});
                var decoder = self.decoders.hasOwnProperty(row.format) ? self.decoders[row.format] : self.defaultDecoder;
                row.payload = decoder(row.payload);
                messages.push(row);
                cb();
            }, function(err) {
                cb(err, messages);
            }
        );
    }

    function pop(messages, cb) {
        // All synchronous
        messages.forEach(function(message){
            self._delete.run(message.uuid);
        });
        cb(null, messages);
    }

    var fns = [get, decode];
    if(!requeue){
        fns.push(pop);
    }

    async.waterfall(fns, function(err, messages){
        callback(err, (err) ? null : messages);
    });
};

/**
 * Closes the database.
 */
Connection.prototype.close = function(callback) {
    var self = this;
    var callback = callback || function(err) {
        if(err) {
            self.logger.error(err, {error: err});
        } else {
            self.logger.debug('Mq service closed.');
        }
    }
    
    if(this._db) {
        async.series({
            insert: function(cb) {
                if(self._insert){
                    self._insert.finalize(cb);   
                } else {
                    setTimeout(cb, 0);
                }
            },
            select: function(cb) {
                if(self._select){
                    self._select.finalize(cb);   
                } else {
                    setTimeout(cb, 0);
                }
            },
            delete: function(cb) {
                if(self._delete){
                    self._delete.finalize(cb);   
                } else {
                    setTimeout(cb, 0);
                }
            },
            db: function(cb) {
                self._db.close(cb);
            }
        }, function(err, res){
            if(err){
                self.logger.error('Error closing the database: %s', err, {});
            }
            self._insert = null;
            self._select = null;
            self._delete = null;
            self._db = null;
            self.emit((err) ? 'error' : 'close', (err) ? err : null);
            callback(err);
        });
    } else {
        setTimeout(callback, 0);
    }
};

module.exports = Connection;

