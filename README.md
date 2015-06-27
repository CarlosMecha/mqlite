# mqlite

A SQLite3 wrapper -experiment- to mock some AMQP functionality.

## Author

Carlos Mecha, 2015

 - Version 0.1: Developed from 06/22/2015 and released on 06/27/2015.

## Methods

 - Producer:
     - `publish`: Sends a message.
 - Consumer:
     - `peek`: Gets a limited number of messages but does not remove them from the queue.
     - `get`: Gets a limited number of messages from the queue.

## Example

```javascript
var mqlite    = require('mqlite');
var conn      = new mqlite.Connection();
var producer  = new mqlite.Producer(conn);
var consumer  = new mqlite.Consumer(conn);
var msg       = new mqlite.Message({topic: 'foo'}, {
    hello: 'world'
});


conn.once('listen', function() {
    producer.publish(msg);
    producer.once('publish', function() {
        consumer.peek('foo', 1);
        consumer.once('message', function(message) {
            console.log(message.payload);
        });
    });
});
```

## Tests
```bash
$ npm test
```

## Contribute
These tiny pieces of code (notifications, mqlite, etc) are ideas or prototypes
developed in ~6 hours. If you find this code useful feel free to do whatever you
want with it. Help/ideas/bug reporting is also welcome.

Thanks!

