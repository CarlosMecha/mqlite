
var assert = require('chai').assert;
var async = require('async');
var expect = require('chai').expect;
var uuidGenerator = require('node-uuid');
var sys = require('sys');

var Connection = require('../lib/connection');
var Message = require('../lib/message');
var Producer = require('../lib/producer');

var conn = null;

describe('Publisher', function(){
    
    beforeEach(function(done){
        conn = new Connection();
        conn.listen(done);
    });

    afterEach(function(done){
        conn.close(done);
    });
    
    describe('initializes the producer', function() {
        it('creates the channel', function(){
            var producer = new Producer(conn);
            expect(producer._connection.channels).to.equals(1);
        });

        it('and closes the channel', function(done){
            var producer = new Producer(conn);
            producer.close(function(){
                expect(producer._connection.channels).to.equals(0);
                done();
            });
        });
    });

    describe('publishes messages', function(){
        it('with a callback', function(done){
            var message = new Message({
                topic: 'test-topic',
                format: 'json'
            }, {foo: 'foo-test'});

            var producer = new Producer(conn);

            async.waterfall([
                function(callback) {
                    producer.publish(message, callback);
                },
                function(uuid, callback) {
                    conn.get(message.headers.topic, 1, false, callback);
                },
                function(res, callback) {
                    expect(res).to.have.length(1);
                    var mes = res[0];
                    expect(mes).to.be.ok;
                    expect(mes.topic).to.be.ok;
                    expect(mes.topic).to.equals(message.headers.topic);
                    expect(mes.uuid).to.be.ok;
                    expect(mes.timestamp).to.be.ok;
                    expect(mes.format).to.be.ok;
                    expect(mes.format).to.equals(message.headers.format);
                    expect(mes.payload).to.be.ok;
                    setImmediate(callback);
                },
                function(callback) {
                    producer.close(callback);
                }
            ], function(err) {
                expect(err).to.not.exists;
                done();
            });

        });    

        it('with events', function(done){
            var message = new Message({
                topic: 'test-topic',
                format: 'json'
            }, {foo: 'foo-test'});

            var producer = new Producer(conn);

            async.waterfall([
                function(callback) {
                    producer.publish(message, callback);
                    producer.once('publish', function(uuid){
                        callback(null, uuid);
                    });
                    producer.once('error', function(err){
                        callback(err);
                    });
                },
                function(uuid, callback) {
                    conn.get(message.headers.topic, 1, false, callback);
                },
                function(res, callback) {
                    expect(res).to.have.length(1);
                    var mes = res[0];
                    expect(mes).to.be.ok;
                    expect(mes.topic).to.be.ok;
                    expect(mes.topic).to.equals(message.headers.topic);
                    expect(mes.uuid).to.be.ok;
                    expect(mes.timestamp).to.be.ok;
                    expect(mes.format).to.be.ok;
                    expect(mes.format).to.equals(message.headers.format);
                    expect(mes.payload).to.be.ok;
                    setImmediate(callback);
                },
                function(callback) {
                    producer.close(callback);
                }
            ], function(err) {
                expect(err).to.not.exists;
                done();
            });

        });    
    });
});


