const Args = require('amqplib/lib/api_args');
const { plugins, constants } = require('hato');
const { Scopes: { CONNECTION, SUBSCRIPTION, PUBLICATION } } = constants;

module.exports = class extends plugins.Base {

    constructor(options = {}) {
        super('log');

        if (typeof options === 'function') {
            options = { log: options };
        }
        this.options = options;

        this.connFields = {};
    }

    // Level a message is logged at, per command. `level` is either one level for both,
    // or `{ publish, consume }`; either defaults to 'info'.
    levelFor(command) {
        const { level = 'info' } = this.options;
        return (typeof level === 'string' ? level : level[command]) || 'info';
    }

    // Whether a part of the logged struct is wanted. Everything is included by default.
    includes(part) {
        const { include = {} } = this.options;
        return include[part] !== false;
    }

    // Applies the `include.properties` choice: `false` drops them, a list of names keeps only those
    selectProperties(properties) {
        const { include = {} } = this.options;
        if (!Array.isArray(include.properties)) {
            return this.includes('properties') ? properties : undefined;
        }
        const selected = {};
        for (const key of include.properties) {
            if (properties && key in properties) selected[key] = properties[key];
        }
        return selected;
    }

    // Asks the user supplied `filter` whether a message is to be logged. It only decides; if it
    // throws, the message is logged anyway rather than lost.
    wanted(entry) {
        const { filter } = this.options;
        if (typeof filter !== 'function') return true;
        try {
            return filter(entry) !== false;
        } catch (err) {
            this.log('warn', { err }, '[AMQP:log] Message filter failed.');
            return true;
        }
    }

    init() {
        this.logger = this.options.logger || this.logger;
        this.log = this.options.log ||
            ((level, struct, msg) => this.logger[level](struct, msg));

        this.scopes[SUBSCRIPTION] = this.onConsume();
        this.scopes[PUBLICATION] = this.onPublish();
        this.scopes[CONNECTION] = this.onConnect();
        this.hooks[CONNECTION] = this.onOpen();
    }

    onConnect() {
        return (connect) => (url, socketOptions) => {
            this.extractProtocol(url);
            return connect(url, socketOptions);
        };
    }

    onOpen() {
        return (conn) => {
            const { connection } = conn,
                { stream: socket, serverProperties } = connection;
            this.log('debug', { serverProperties }, 'Connected to broker.');
            Object.assign(this.connFields, {
                protocolVersion: '0.9.1',
                localAddress: socket.localAddress,
                localPort: socket.localPort,
                remoteAddress: socket.remoteAddress,
                remotePort: socket.remotePort,
                system: `${serverProperties.product} ${serverProperties.version}`
            });
        };
    }

    onConsume() {
        const plugin = this;
        return (consume) => (queue, fn, options) => {
            if (options && options.silent) return consume(queue, fn, options);
            const handler = function(msg) {
                const { fields, properties, content } = msg;
                // Log with the provided function
                try {
                    if (plugin.wanted({
                        command: 'consume',
                        queue,
                        exchange: fields.exchange,
                        routingKey: fields.routingKey,
                        properties
                    })) {
                        const msg = plugin.getMessage('consume',
                            fields.exchange, fields.routingKey, content, queue);
                        plugin.log(plugin.levelFor('consume'), {
                            ...(plugin.includes('connection') && plugin.connFields),
                            command: 'consume',
                            queue,
                            options: plugin.includes('options') ? options : undefined,
                            exchange: fields.exchange,
                            routingKey: fields.routingKey,
                            fields: plugin.includes('fields') ? fields : undefined,
                            properties: plugin.selectProperties(properties),
                            content: plugin.serializeContent(content)
                        }, msg);
                    }
                } catch (err) {
                    plugin.log('warn', { err }, '[AMQP:log] Message logging failed.');
                }

                return fn(msg);
            };
            return consume(queue, handler, options);
        };
    }

    onPublish() {
        const plugin = this;
        return (publish) => (exchange, routingKey, content, options, cb) => {
            if (typeof cb !== 'function') {
                const ok = publish(exchange, routingKey, content, options);
                plugin.logPublish(exchange, routingKey, content, options);
                return ok;
            }
            return publish(exchange, routingKey, content, options, function(err) {
                cb(err);
                plugin.logPublish(exchange, routingKey, content, options, err);
            });
        };
    }

    logPublish(exchange, routingKey, content, options, err) {
        // Log with the provided function
        try {
            if (options && options.silent) return;
            const properties = Args.publish(exchange, routingKey, options);
            const logged = {
                ...properties,
                headers: Object.getPrototypeOf(properties.headers)
            };
            if (!this.wanted({ command: 'publish', exchange, routingKey, properties: logged })) return;
            const msg = this.getMessage(
                'publish', exchange, routingKey, content);
            this.log(this.levelFor('publish'), {
                ...(this.includes('connection') && this.connFields),
                err,
                command: 'publish',
                options: this.includes('options') ? options : undefined,
                exchange,
                routingKey,
                properties: this.selectProperties(logged),
                content: this.serializeContent(content)
            }, msg);
        } catch (err) {
            this.log('warn', { err }, '[AMQP:log] Message logging failed.');
        }
    }

    extractProtocol(url) {
        if (typeof url === 'object') {
            this.connFields.protocol = url.protocol;
        } else if (typeof url === 'string') {
            url = new URL(url);
            this.connFields.protocol = url.protocol.replace(/:$/, '');
        }
    }

    getMessage(command, exchange, routingKey, content, queue) {
        const {
            protocolVersion,
            system
        } = this.connFields;
        const contentLen = Buffer.isBuffer(content) ? `${content.length}B` : '-';
        const nodes = [];
        if (command === 'consume' && exchange) {
            nodes.push(exchange, routingKey, queue);
        } else {
            // publish or default exchange, in which case queue = routing key
            nodes.push(exchange || '(default)', routingKey);
        }
        const flow = `[${command}] ${nodes.join(' -> ')}`;
        return `AMQP/${protocolVersion} ${flow} ${contentLen} "${system}"`;
    }

    serializeContent(content) {
        try {
            const buf = Buffer.isBuffer(content) ?
                content : JSON.stringify(content);
            const {
                enabled = false,
                maxBytes = 1000
            } = this.options.body || {};
            if (!enabled || buf.length > maxBytes) return;
            try {
                return JSON.parse(buf);
            } catch {
                return buf.toString('utf8');
            }
        } catch {
            return undefined;
        }
    }

};
