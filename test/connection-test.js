
var assert = require('chai').assert;
var async = require('async');
var expect = require('chai').expect;
var uuidGenerator = require('node-uuid');
var sys = require('sys');

var Connection = require('../lib/connection');

describe('Connection', function(){
    describe('initializes the ephemeral database', function() {
        it('creates the database', function(done){
            var mq = new Connection();
            mq.listen(function(err){
                expect(err).to.not.exists;
                expect(mq._db.open).to.be.true;
                expect(mq._db.filename).to.equal(':memory:');
                mq.close(done);
            });
        });

        it('inserts the schema with a messages table', function(done){
            var mq = new Connection();
            mq.listen(function(err){
                expect(err).to.not.exists;
                var db = mq._db;
                mq._db.get('SELECT 1 FROM messages', function(err, row) {
                    expect(err).to.not.exists;
                    expect(row).to.not.be.ok;
                    setTimeout(function(){
                        mq.close(done);
                    });
                });
            });
            
        });

        it('and closes the database', function(done){
            var mq = new Connection();
            mq.listen(function(err){
                expect(err).to.not.exists;
                var db = mq._db;
                mq.close(function(err){
                    expect(err).to.not.exists;
                    expect(db.open).to.be.false;
                    done();
                });
            });
        });

        it('emites the event when listen and closed', function(done){
            var mq = new Connection();
            mq.listen();
            mq.once('listen', function(){
                mq.close();
                mq.once('close', done);
            });
        });
    });

    describe('initializes the persistent database', function() {

        var filename = 'test.db';
        
        it('creates the database', function(done){
            var mq = new Connection(filename);
            mq.listen(function(err){
                expect(err).to.not.exists;
                expect(mq._db.open).to.be.true;
                expect(mq._db.filename).to.equal(filename);
                mq.close(done);
            });

        });

        it('inserts the schema with a messages table', function(done){
            var mq = new Connection(filename);
            mq.listen(function(err){
                expect(err).to.not.exists;
                var db = mq._db;
                mq._db.get('SELECT 1 FROM messages', function(err, row) {
                    expect(err).to.not.exists;
                    expect(row).to.not.be.ok;
                    setTimeout(function(){
                        mq.close(done);
                    });
                });
            });
            
        });

        it('and closes the database', function(done){
            var mq = new Connection(filename);
            mq.listen(function(err){
                expect(err).to.not.exists;
                var db = mq._db;
                mq.close(function(err){
                    expect(err).to.not.exists;
                    expect(db.open).to.be.false;
                    done();
                });
            });
        });

        it('emites the event when listen and closed', function(done){
            var mq = new Connection(filename);
            mq.listen();
            mq.once('listen', function(){
                mq.close();
                mq.once('close', done);
            });
        });
    });

    describe('stores messages', function(){

        var mq = null;

        beforeEach(function(done) {
            mq = new Connection();
            mq.listen();
            mq.once('listen', done);
        });

        afterEach(function(done) {
            mq.close();
            mq.once('close', done);
        });

        it('with the default encoder', function(done){
            var topic = 'test-topic';
            var format = 'unknown';
            var payload = {foo: 'foo-test'};

            async.waterfall([
                function(callback) {
                    mq._push(topic, format, payload, callback);
                },
                function(uuid, callback) {
                    var db = mq._db;
                    db.get('SELECT topic, payload FROM messages WHERE uuid = ?', uuid, function(err, row) {
                        if(err){
                            callback(err);
                        } else {
                            expect(row).to.be.ok;
                            expect(row.payload).to.be.ok;
                            expect(row.topic).to.be.ok;
                            expect(row.topic).to.equals(topic);
                            var obj = JSON.parse(row.payload);
                            expect(obj).to.be.ok;
                            expect(obj.foo).to.be.ok;
                            expect(obj.foo).to.equals(payload.foo);
                            callback();
                        }
                    });
                }
            ], function(err) {
                expect(err).to.not.exists;
                done();
            });

        });    

        it('with a custom encoder', function(done){
            var topic = 'test-topic';
            var format = 'custom';
            var payload = {foo: 'foo-test'};

            mq.encoders = { 'custom' : function(obj) {
                return "1" + JSON.stringify(obj);
            }};
            mq.decoders = { 'custom' : function(obj) {
                return JSON.parse(obj.substring(1));
            }};

            async.waterfall([
                function(callback) {
                    mq._push(topic, format, payload, callback);
                },
                function(uuid, callback) {
                    var db = mq._db;
                    db.get('SELECT topic, payload FROM messages WHERE uuid = ?', uuid, function(err, row) {
                        if(err){
                            callback(err);
                        } else {
                            expect(row).to.be.ok;
                            expect(row.payload).to.be.ok;
                            expect(row.topic).to.be.ok;
                            expect(row.topic).to.equals(topic);
                            var obj = JSON.parse(row.payload.substring(1));
                            expect(obj).to.be.ok;
                            expect(obj.foo).to.be.ok;
                            expect(obj.foo).to.equals(payload.foo);
                            callback();
                        }
                    });
                }
            ], function(err) {
                expect(err).to.not.exists;
                done();
            });

        });    
    });

    describe('retrieves messages', function(){

        var mq = null;

        beforeEach(function(done) {
            mq = new Connection();
            mq.listen();
            mq.once('listen', done);
        });

        afterEach(function(done) {
            mq.close();
            mq.once('close', done);
        });

        it('with the default decoder', function(done){
            var topic = 'test-topic';
            var format = 'unknown';
            var payload = {foo: 'foo-test'};
            var uuid = uuidGenerator.v1();
            var timestamp = Date.now();
            var limit = 1;

            async.waterfall([
                function(callback) {
                    var db = mq._db;
                    db.run(
                        'INSERT INTO messages (uuid, topic, timestamp, format, payload) VALUES (?, ?, ?, ?, ?)',
                        uuid, topic, timestamp, format, JSON.stringify(payload), callback
                    );
                },
                function(callback) {
                    mq._get(topic, limit, true, callback);
                },
                function(res, callback) {
                    expect(res).to.be.ok;
                    expect(res).to.have.length(limit);
                    var notif = res[0];
                    expect(notif).to.be.ok;
                    expect(notif.topic).to.be.ok;
                    expect(notif.topic).to.equals(topic);
                    expect(notif.uuid).to.be.ok;
                    expect(notif.uuid).to.equals(uuid);
                    expect(notif.timestamp).to.be.ok;
                    expect(notif.timestamp).to.equals(timestamp);
                    expect(notif.format).to.be.ok;
                    expect(notif.format).to.equals(format);
                    expect(notif.payload).to.be.ok;
                    var obj = JSON.parse(notif.payload);
                    expect(obj).to.be.ok;
                    expect(obj.foo).to.be.ok;
                    expect(obj.foo).to.equals(payload.foo);
                    callback();
                },
                function(callback) {
                    mq._get(topic, limit, true, callback);
                },
                function(res, callback) {
                    expect(res).to.be.ok;
                    expect(res).to.have.length(limit);
                    callback();
                }
            ], function(err) {
                expect(err).to.not.exists;
                done();
            });
        });

        it('with a custom decoder', function(done){
            var topic = 'test-topic';
            var format = 'custom';
            var payload = {foo: 'foo-test'};
            var uuid = uuidGenerator.v1();
            var timestamp = Date.now();
            var limit = 1;

            mq.encoders = { 'custom' : function(obj) {
                return "1" + JSON.stringify(obj);
            }};
            mq.decoders = { 'custom' : function(obj) {
                return JSON.parse(obj.substring(1));
            }};

            async.waterfall([
                function(callback) {
                    mq.listen(callback);
                },
                function(callback) {
                    var db = mq._db;
                    db.run(
                        'INSERT INTO messages (uuid, topic, timestamp, format, payload) VALUES (?, ?, ?, ?, ?)',
                        uuid, topic, timestamp, format, mq.encoders.custom(payload), callback
                    );
                },
                function(callback) {
                    mq._get(topic, limit, true, callback);
                },
                function(res, callback) {
                    expect(res).to.be.ok;
                    expect(res).to.have.length(limit);
                    var notif = res[0];
                    expect(notif).to.be.ok;
                    expect(notif.topic).to.be.ok;
                    expect(notif.topic).to.equals(topic);
                    expect(notif.uuid).to.be.ok;
                    expect(notif.uuid).to.equals(uuid);
                    expect(notif.timestamp).to.be.ok;
                    expect(notif.timestamp).to.equals(timestamp);
                    expect(notif.format).to.be.ok;
                    expect(notif.format).to.equals(format);
                    expect(notif.payload).to.be.ok;
                    callback();
                },
                function(callback) {
                    mq._get(topic, limit, true, callback);
                },
                function(res, callback) {
                    expect(res).to.be.ok;
                    expect(res).to.have.length(limit);
                    callback();
                }
            ], function(err) {
                expect(err).to.not.exists;
                done();
            });
        });

        it('and requeues', function(done){
            var topic = 'test-topic';
            var format = 'json';
            var payload = {foo: 'foo-test'};
            var uuid = uuidGenerator.v1();
            var timestamp = Date.now();
            var limit = 1;

            function get(callback) {
                mq._get(topic, limit, true, callback);
            }

            function check(res, callback) {
                expect(res).to.be.ok;
                expect(res).to.have.length(limit);
                var notif = res[0];
                expect(notif).to.be.ok;
                expect(notif.topic).to.be.ok;
                expect(notif.topic).to.equals(topic);
                expect(notif.uuid).to.be.ok;
                expect(notif.uuid).to.equals(uuid);
                expect(notif.timestamp).to.be.ok;
                expect(notif.timestamp).to.equals(timestamp);
                expect(notif.format).to.be.ok;
                expect(notif.format).to.equals(format);
                expect(notif.payload).to.be.ok;
                var obj = JSON.parse(notif.payload);
                expect(obj).to.be.ok;
                expect(obj.foo).to.be.ok;
                expect(obj.foo).to.equals(payload.foo);
                callback();
            }

            async.waterfall([
                function(callback) {
                    var db = mq._db;
                    db.run(
                        'INSERT INTO messages (uuid, topic, timestamp, format, payload) VALUES (?, ?, ?, ?, ?)',
                        uuid, topic, timestamp, format, JSON.stringify(payload), callback
                    );
                },
                get,
                check,
                get,
                check
            ], function(err) {
                expect(err).to.not.exists;
                done();
            });

        });

        it('and not requeues', function(done){
            var topic = 'test-topic';
            var format = 'json';
            var payload = {foo: 'foo-test'};
            var uuid = uuidGenerator.v1();
            var timestamp = Date.now();
            var limit = 1;

            async.waterfall([
                function(callback) {
                    var db = mq._db;
                    db.run(
                        'INSERT INTO messages (uuid, topic, timestamp, format, payload) VALUES (?, ?, ?, ?, ?)',
                        uuid, topic, timestamp, format, JSON.stringify(payload), callback
                    );
                },
                function(callback) {
                    mq._get(topic, limit, false, callback);
                },
                function(res, callback) {
                    expect(res).to.be.ok;
                    expect(res).to.have.length(limit);
                    var notif = res[0];
                    expect(notif).to.be.ok;
                    expect(notif.topic).to.be.ok;
                    expect(notif.topic).to.equals(topic);
                    expect(notif.uuid).to.be.ok;
                    expect(notif.uuid).to.equals(uuid);
                    expect(notif.timestamp).to.be.ok;
                    expect(notif.timestamp).to.equals(timestamp);
                    expect(notif.format).to.be.ok;
                    expect(notif.format).to.equals(format);
                    expect(notif.payload).to.be.ok;
                    var obj = JSON.parse(notif.payload);
                    expect(obj).to.be.ok;
                    expect(obj.foo).to.be.ok;
                    expect(obj.foo).to.equals(payload.foo);
                    callback();
                },
                function(callback) {
                    mq._get(topic, limit, true, callback);
                },
                function(res, callback) {
                    expect(res).to.be.ok;
                    expect(res).to.have.length(0);
                    callback();
                }
            ], function(err) {
                expect(err).to.not.exists;
                done();
            });

        });
    });
});


