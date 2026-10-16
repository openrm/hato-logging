import hato from 'hato';

export as namespace hatoLogging;

type LogLevel = 'debug' | 'info' | 'warn';
type LogTransform = (level: LogLevel, struct: object, msg: string) => void;
type Logger = {
    [key in LogLevel]: (struct: object, msg: string) => void;
};

/** What is known about a message before it is logged, handed to `filter`. */
type MessageInfo = {
    command: 'publish' | 'consume';
    exchange: string;
    routingKey: string;
    /** Only for consumed messages */
    queue?: string;
    properties: { [key: string]: any };
};

type Include = {
    /** Protocol, addresses and broker product of the connection. Default: true */
    connection?: boolean;
    /** The `options` the message was published or consumed with. Default: true */
    options?: boolean;
    /** The delivery `fields` of a consumed message. Default: true */
    fields?: boolean;
    /** The message properties; a list of names keeps only those. Default: true */
    properties?: boolean | string[];
};

type Options = LogTransform | {
    log?: LogTransform;
    logger?: Logger;
    /** Log the content of messages up to `maxBytes`. Default: disabled */
    body?: { enabled?: boolean; maxBytes?: number };
    /** Level to log messages at, for both commands or for each. Default: 'info' */
    level?: LogLevel | { publish?: LogLevel; consume?: LogLevel };
    /** Leave parts out of the logged struct. Everything is included by default */
    include?: Include;
    /** Return `false` to skip logging a message. Anything else logs it */
    filter?: (message: MessageInfo) => boolean | void;
};
declare const Plugin: hato.Plugins.Plugin<Options>;

export = Plugin;
