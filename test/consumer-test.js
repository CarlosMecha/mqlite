
var assert = require('chai').assert;
var async = require('async');
var expect = require('chai').expect;
var uuidGenerator = require('node-uuid');
var sys = require('sys');

var Connection = require('../lib/connection');
var Message = require('../lib/message');
var Consumer = require('../lib/consumer');

var conn = null;

describe('Consumer', function(){
    
    beforeEach(function(done){
        conn = new Connection();
        conn.listen(done);
    });

    afterEach(function(done){
        conn.close(done);
    });
    
    describe('initializes the consumer', function() {
        it('creates the channel', function(){
            var consumer = new Consumer(conn);
            var id = consumer.channel.id;
            expect(consumer._connection.channels[id]).to.be.ok;
        });

        it('and closes the channel', function(done){
            var consumer = new Consumer(conn);
            var id = consumer.channel.id;
            consumer.close(function(){
                expect(consumer._connection.channels[id]).to.be.undefined;
                done();
            });
        });
    });


    describe('peeks messages', function(){
        it('with a callback', function(done){
            var topic = 'test-topic';
            var format = 'unknown';
            var payload = {foo: 'foo-test'};
            var uuid = null;

            var consumer = new Consumer(conn);

            async.waterfall([
                function(callback) {
                    conn._push(topic, format, payload, callback);
                },
                function(res, callback) {
                    uuid = res;
                    consumer.peek(topic, 1, callback);
                },
                function(res, callback) {
                    expect(res).to.have.length(1);
                    var mes = res[0];
                    expect(mes).to.be.ok;
                    expect(mes.headers.topic).to.be.ok;
                    expect(mes.headers.topic).to.equals(topic);
                    expect(mes.headers.uuid).to.be.ok;
                    expect(mes.headers.uuid).to.equals(uuid);
                    expect(mes.headers.timestamp).to.be.ok;
                    expect(mes.headers.format).to.be.ok;
                    expect(mes.headers.format).to.equals(format);
                    expect(mes.payload).to.be.ok;
                    setImmediate(callback);
                },
                function(callback) {
                    conn._get(topic, 1, false, callback);
                },
                function(res, callback) {
                    expect(res).to.have.length(1);
                    expect(res[0].uuid).to.be.ok;
                    setImmediate(callback);
                },
                function(callback) {
                    consumer.close(callback);
                }
            ], function(err) {
                expect(err).to.not.exists;
                done();
            });

        });    

        it('with events', function(done){
            var topic = 'test-topic';
            var format = 'unknown';
            var payload = {foo: 'foo-test'};
            var uuid = null;

            var consumer = new Consumer(conn);

            async.waterfall([
                function(callback) {
                    conn._push(topic, format, payload, callback);
                },
                function(res, callback) {
                    uuid = res;
                    consumer.peek(topic, 1);
                    consumer.once('message', function(message){
                        callback(null, message);
                    });
                    consumer.once('error', function(err){
                        callback(err);
                    });
                },
                function(mes, callback) {
                    expect(mes).to.be.ok;
                    expect(mes.headers.topic).to.be.ok;
                    expect(mes.headers.topic).to.equals(topic);
                    expect(mes.headers.uuid).to.be.ok;
                    expect(mes.headers.uuid).to.equals(uuid);
                    expect(mes.headers.timestamp).to.be.ok;
                    expect(mes.headers.format).to.be.ok;
                    expect(mes.headers.format).to.equals(format);
                    expect(mes.payload).to.be.ok;
                    setImmediate(callback);
                },
                function(callback) {
                    consumer.close(callback);
                }
            ], function(err) {
                expect(err).to.not.exists;
                done();
            });

        });    
    });

    describe('retrieves messages', function(){
        it('with a callback', function(done){
            var topic = 'test-topic';
            var format = 'unknown';
            var payload = {foo: 'foo-test'};
            var uuid = null;

            var consumer = new Consumer(conn);

            async.waterfall([
                function(callback) {
                    conn._push(topic, format, payload, callback);
                },
                function(res, callback) {
                    uuid = res;
                    consumer.get(topic, 1, callback);
                },
                function(res, callback) {
                    expect(res).to.have.length(1);
                    var mes = res[0];
                    expect(mes).to.be.ok;
                    expect(mes.headers.topic).to.be.ok;
                    expect(mes.headers.topic).to.equals(topic);
                    expect(mes.headers.uuid).to.be.ok;
                    expect(mes.headers.uuid).to.equals(uuid);
                    expect(mes.headers.timestamp).to.be.ok;
                    expect(mes.headers.format).to.be.ok;
                    expect(mes.headers.format).to.equals(format);
                    expect(mes.payload).to.be.ok;
                    setImmediate(callback);
                },
                function(callback) {
                    consumer.close(callback);
                }
            ], function(err) {
                expect(err).to.not.exists;
                done();
            });

        });    

        it('with events', function(done){
            var topic = 'test-topic';
            var format = 'unknown';
            var payload = {foo: 'foo-test'};
            var uuid = null;

            var consumer = new Consumer(conn);

            async.waterfall([
                function(callback) {
                    conn._push(topic, format, payload, callback);
                },
                function(res, callback) {
                    uuid = res;
                    consumer.get(topic, 1);
                    consumer.once('message', function(message){
                        callback(null, message);
                    });
                    consumer.once('error', function(err){
                        callback(err);
                    });
                },
                function(mes, callback) {
                    expect(mes).to.be.ok;
                    expect(mes.headers.topic).to.be.ok;
                    expect(mes.headers.topic).to.equals(topic);
                    expect(mes.headers.uuid).to.be.ok;
                    expect(mes.headers.uuid).to.equals(uuid);
                    expect(mes.headers.timestamp).to.be.ok;
                    expect(mes.headers.format).to.be.ok;
                    expect(mes.headers.format).to.equals(format);
                    expect(mes.payload).to.be.ok;
                    setImmediate(callback);
                },
                function(callback) {
                    consumer.close(callback);
                }
            ], function(err) {
                expect(err).to.not.exists;
                done();
            });

        });    
    });
});


