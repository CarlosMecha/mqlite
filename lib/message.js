
/**
 * Message.
 */
function Message(headers, payload) {
    this._rawPayload = null;
    this.headers = {
        timestamp: new Date()
    };
    for (var attrname in headers) {
        this.headers[attrname] = headers[attrname];
    }
    this.payload = payload;
}

module.exports = Message;

