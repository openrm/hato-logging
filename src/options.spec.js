const assert = require('assert');

const Log = require('.');

// Exercises the plugin without a broker, by driving the wrapped consume and publish functions
describe('log plugin options', () => {
    const connFields = {
        protocol: 'amqp',
        protocolVersion: '0.9.1',
        localAddress: '10.0.0.1',
        localPort: 1234,
        remoteAddress: '10.0.0.2',
        remotePort: 5672,
        system: 'RabbitMQ 3.9.29'
    };

    function setup(options) {
        const logged = [];
        const plugin = new Log({ log: (level, data, msg) => logged.push({ level, data, msg }), ...options });
        plugin.init();
        Object.assign(plugin.connFields, connFields);
        return { plugin, logged };
    }

    function consume(plugin, { queue = 'q', exchange = 'amq.direct', routingKey = 'a.key', properties = {} } = {}) {
        const fields = { consumerTag: 'ctag', deliveryTag: 1, redelivered: false, exchange, routingKey };
        const handler = plugin.onConsume()((_queue, fn) => fn)(queue, () => {}, {});
        handler({ fields, properties, content: Buffer.from('{}') });
    }

    function publish(plugin, options = {}) {
        plugin.onPublish()(() => true)('amq.direct', 'a.key', Buffer.from('{}'), options);
    }

    describe('by default', () => {
        it('logs everything at info, as before', () => {
            const { plugin, logged } = setup();
            consume(plugin, { properties: { correlationId: 'c1', replyTo: 'amq.gen-x' } });
            publish(plugin, { correlationId: 'c1' });

            assert.deepStrictEqual(logged.map((l) => l.level), ['info', 'info']);
            for (const { data } of logged) {
                for (const key of Object.keys(connFields)) assert.strictEqual(data[key], connFields[key]);
                assert('options' in data);
                assert('properties' in data);
            }
            assert.strictEqual(logged[0].data.fields.deliveryTag, 1);
            assert.strictEqual(logged[0].data.properties.replyTo, 'amq.gen-x');
        });
    });

    describe('level', () => {
        it('applies one level to both commands', () => {
            const { plugin, logged } = setup({ level: 'debug' });
            consume(plugin);
            publish(plugin);
            assert.deepStrictEqual(logged.map((l) => l.level), ['debug', 'debug']);
        });

        it('applies a level per command and falls back to info', () => {
            const { plugin, logged } = setup({ level: { consume: 'debug' } });
            consume(plugin);
            publish(plugin);
            assert.deepStrictEqual(logged.map((l) => l.level), ['debug', 'info']);
        });
    });

    describe('include', () => {
        it('leaves out the connection fields', () => {
            const { plugin, logged } = setup({ include: { connection: false } });
            consume(plugin);
            publish(plugin);
            for (const { data } of logged) {
                for (const key of Object.keys(connFields)) assert(!(key in data), key);
                assert.strictEqual(data.routingKey, 'a.key');
            }
        });

        it('leaves out options and delivery fields', () => {
            const { plugin, logged } = setup({ include: { options: false, fields: false } });
            consume(plugin);
            publish(plugin);
            for (const { data } of logged) assert.strictEqual(data.options, undefined);
            assert.strictEqual(logged[0].data.fields, undefined);
        });

        it('keeps only the listed properties', () => {
            const { plugin, logged } = setup({ include: { properties: ['correlationId'] } });
            consume(plugin, { properties: { correlationId: 'c1', replyTo: 'amq.gen-x', headers: {} } });
            publish(plugin, { correlationId: 'c2', headers: { a: 1 } });
            assert.deepStrictEqual(logged[0].data.properties, { correlationId: 'c1' });
            assert.deepStrictEqual(logged[1].data.properties, { correlationId: 'c2' });
        });

        it('leaves out properties altogether', () => {
            const { plugin, logged } = setup({ include: { properties: false } });
            consume(plugin, { properties: { correlationId: 'c1' } });
            publish(plugin, { correlationId: 'c2' });
            for (const { data } of logged) assert.strictEqual(data.properties, undefined);
        });

        it('treats anything not set to false as included', () => {
            const { plugin, logged } = setup({ include: { connection: true, fields: undefined } });
            consume(plugin);
            assert.strictEqual(logged[0].data.protocol, 'amqp');
            assert(logged[0].data.fields);
        });
    });

    describe('filter', () => {
        it('skips what the filter rejects', () => {
            const seen = [];
            const { plugin, logged } = setup({
                filter: (message) => {
                    seen.push(message);
                    return !String(message.queue || message.routingKey).startsWith('amq.gen-');
                }
            });
            consume(plugin, { queue: 'amq.gen-abc', exchange: '', routingKey: 'amq.gen-abc' });
            consume(plugin, { queue: 'svc:a.key' });
            publish(plugin);

            assert.strictEqual(logged.length, 2);
            assert.deepStrictEqual(seen.map((m) => m.command), ['consume', 'consume', 'publish']);
            assert.strictEqual(seen[0].queue, 'amq.gen-abc');
            assert.strictEqual(seen[2].exchange, 'amq.direct');
        });

        it('logs when the filter returns nothing', () => {
            const { plugin, logged } = setup({ filter: () => {} });
            consume(plugin);
            publish(plugin);
            assert.strictEqual(logged.length, 2);
        });

        it('logs, with a warning, when the filter throws', () => {
            const { plugin, logged } = setup({ filter: () => { throw new Error('boom'); } });
            consume(plugin);
            assert.deepStrictEqual(logged.map((l) => l.level), ['warn', 'info']);
            assert.strictEqual(logged[0].data.err.message, 'boom');
        });
    });

    describe('silent', () => {
        it('still skips consumers that are silent', () => {
            const { plugin } = setup();
            const fn = () => {};
            // a silent consumer is handed through unwrapped, so nothing can be logged for it
            const handler = plugin.onConsume()((_queue, fn) => fn)('q', fn, { silent: true });
            assert.strictEqual(handler, fn);
        });
    });
});
