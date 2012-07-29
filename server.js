#!/usr/bin/env node

var shorty  = require('shorty'),
    smpp    = shorty.getSmppDefinitions(),
    remote  = require('./lib/remote'),
    net     = require('net'),
    fs      = require('fs'),
    path    = require('path'),
    Log     = require('log'),
    Redback = require('redback'),
    uuid    = require('node-uuid'),
    cluster = require('cluster'),
    daemon  = require('daemon'),
    args    = require('optimist').default('daemon', true).default('dev', false).argv,
    fsext   = require('fs-ext'),
    configFile, config, log;

if (args.dev) {
    configFile = 'server.dev.json';
} else {
    configFile = 'server.json';
}

try {
    config = JSON.parse(fs.readFileSync(configFile).toString());
} catch (err) {
    configFile = path.join(path.dirname(args[1]), configFile);

    try {
        var config = JSON.parse(fs.readFileSync(configFile).toString());
    } catch (err2) {
        console.log("Couldn't find/read/parse config file");
        process.exit(1);
    }
}

if (args.stop) {
    try {
        var pid = fs.readFileSync(config.pidFile).toString();
        process.kill(pid);
    } catch (err) {
        console.log("Process not running");
    }

    try {
        fs.unlinkSync(config.pidFile);
    } catch (err) {}

    process.exit(0);
}

if (args.daemon) {
    log = new Log(Log.INFO, fs.createWriteStream(config.outFile, {flags: 'a'}));
} else {
    log = new Log(Log.DEBUG);
}

remote.setMaxSockets(config.max_http_requests);

if (cluster.isMaster) {
    if (args.daemon) {
        var fd = fs.openSync(config.pidFile, 'w');
        try {
            // try to get a exclusive lock (non-blocking)
            fsext.flockSync(fd, 'exnb');
            fs.close(fd);
        } catch (err) {
            console.log('Coud not acquire exclusive lock on ' + config.pidFile);
            process.exit(1);
        }

        daemon.start(process.stdout.fd);

        var lock = daemon.lock(config.pidFile);
        if (lock === false) {
            log.emergency('Someone else locked the pidfile!');
            process.exit(1);
        }
    }

    var workers = [],
        numWorkers = require('os').cpus().length,
        deadWorkers = 0;
        clients = {},
        redback = Redback.createClient(),
        redbackPubSub = Redback.createClient(),
        channel = redbackPubSub.createChannel('server:sms-mo').subscribe(),
        moQueues = {};

    redback.addStructure('SimpleQueue', {
        add: function(value, callback) {
            this.client.lpush(this.key, value, callback);
        },
        next: function(callback) {
            this.client.lpop(this.key, callback);
        },
        unpop: function(value, callback) {
            this.client.rpush(this.key, value, callback);
        }
    });

    var pop = function(queue, system_id) {
        queue.next(function(err, resp) {
            if (err === null && resp === null) {
                // Nothing in this queue
                return;
            } else {
                if (clients[system_id] !== undefined) {
                    // Prefer receivers over transceivers for MOs
                    if (Object.keys(clients[system_id].receivers).length !== 0) {
                        var len = Object.keys(clients[system_id].receivers).length,
                            index
                            key;

                        clients[system_id].lastReceiverIndex++;
                        if (clients[system_id].lastReceiverIndex >= len) {
                            clients[system_id].lastReceiverIndex = 0;
                            index = 0;
                        } else {
                            index = clients[system_id].lastReceiverIndex;
                        }

                        key = Object.keys(clients[system_id].receivers)[index];

                        // Send to the worker
                        clients[system_id].receivers[key].send({cmd: "mo", data: resp});
                    } else if (Object.keys(clients[system_id].transceivers).length !== 0) {
                        var len = Object.keys(clients[system_id].transceivers).length,
                            index,
                            key;

                        clients[system_id].lastTransceiverIndex++;
                        if (clients[system_id].lastTransceiverIndex >= len) {
                            clients[system_id].lastTransceiverIndex = 0;
                            index = 0;
                        } else {
                            index = clients[system_id].lastTransceiverIndex;
                        }

                        key = Object.keys(clients[system_id].transceivers)[index];

                        // Send to the worker
                        clients[system_id].transceivers[key].send({cmd: "mo", data: resp});
                    } else {
                        queue.unpop(resp);
                        return;
                    }

                    pop(queue, system_id);
                } else {
                    queue.unpop(resp);
                    return;
                }
            }
        });
    };

    channel.on('message', function(msg) {
        var message;
        try {
            message = JSON.parse(msg);

            if (message.command && message.command == "mo") {
                if (moQueues[message.system_id] === undefined) {
                    moQueues[message.system_id] = redback.createSimpleQueue('mo:' + message.system_id);
                }

                process.nextTick(function() {
                    pop(moQueues[message.system_id], message.system_id);
                });
            }

        } catch (ex) {
            log.error("got bad JSON from redis pubsub channel server:sms-mo");
        }
    });

    for (var i = 0; i < numWorkers; i++) {
        (function() {
            var worker = cluster.fork();
            workers.push(worker);
            worker.on('message', function(msg) {
                if (msg.cmd && msg.cmd == 'bind') {
                    if (clients[msg.system_id] === undefined) {
                        clients[msg.system_id] = {
                            receivers: {},
                            lastReceiverIndex: 0,
                            transmitters: {},
                            transceivers: {},
                            lastTransceiverIndex: 0
                        };
                    }

                    if (msg.bind_type == smpp.RECEIVER) {
                        clients[msg.system_id].receivers[msg.connection_uuid] = worker;
                    } else if (msg.bind_type == smpp.TRANSMITTER) {
                        clients[msg.system_id].transmitters[msg.connection_uuid] = worker;
                    } else if (msg.bind_type == smpp.TRANSCEIVER) {
                        clients[msg.system_id].transceivers[msg.connection_uuid] = worker;
                    }

                    if (moQueues[msg.system_id] === undefined) {
                        moQueues[msg.system_id] = redback.createSimpleQueue('mo:' + msg.system_id);
                    }

                    process.nextTick(function() {
                        pop(moQueues[msg.system_id], msg.system_id);
                    });
                } else if (msg.cmd && msg.cmd == 'disconnect') {
                    var bind_type;
                    if (msg.bind_type == smpp.RECEIVER) {
                        bind_type = "receivers";
                    } else if (msg.bind_type == smpp.TRANSMITTER) {
                        bind_type = "transmitters";
                    } else if (msg.bind_type == smpp.TRANSCEIVER) {
                        bind_type = "transceivers";
                    } else {
                        // in this case, the client was never actually bound, so
                        // they will not be registered with the master
                        return;
                    }

                    if (clients[msg.system_id][bind_type][msg.connection_uuid] !== undefined) {
                        delete clients[msg.system_id][bind_type][msg.connection_uuid];
                    }
                }
            });
        })();
    }

    cluster.on('death', function(worker) {
        if (worker.exitCode !== 0) {
            log.alert("worker " + worker.pid + " died of unknown causes (exit code " + worker.exitCode + ")");
        }

        deadWorkers++;

        if (deadWorkers == numWorkers) {
            process.exit(0);
        }
    });

    var sighandle = function() {
        for (var i = 0; i < workers.length; i++) {
            workers[i].kill();
        }
    };

    process.on('SIGHUP', sighandle);
    process.on('SIGINT', sighandle);
    process.on('SIGQUIT', sighandle);
    process.on('SIGTERM', sighandle);
    process.on('SIGSTOP', sighandle);

    // rotate logs on SIGUSR1
    process.on('SIGUSR1', function() {
        if (args.daemon) {
            log = new Log(Log.INFO, fs.createWriteStream(config.logPath, {flags: 'a'}));
        } else {
            log = new Log(Log.DEBUG);
        }

        for (var i = 0; i < workers.length; i++) {
            workers[i].send({cmd:"rotate-logs"});
        }
    });
} else {
    var shortyServer = shorty.createServer(config.shorty);

    process.on('message', function(msg) {
        if (msg.cmd && msg.cmd == 'mo') {
            var length, data = JSON.parse(msg.data);

            if (Buffer.isBuffer(data.message)) {
                length = data.message.length;
            } else {
                if (typeof data.message != "string") {
                    try {
                        data.message.toString();
                    } catch (err) {
                        return;
                    }
                }

                length = Buffer.byteLength(data.message);
            }

            shortyServer.deliverMessage(data.system_id, {
                destination_addr: data.to.replace('+',''),
                dest_addr_ton: 0,
                source_addr: data.from.replace('+',''),
                source_addr_ton: 0,
                short_message: data.message,
                sm_length: length
            });
        } else if (msg.cmd && msg.cmd === 'rotate-logs') {
            if (args.daemon) {
                log = new Log(Log.INFO, fs.createWriteStream(config.logPath, {flags: 'a'}));
            } else {
                log = new Log(Log.DEBUG);
            }
        }
    });

    shortyServer.on('bind', function(client, pdu, callback) {
        var system_id = pdu.system_id.toString('ascii'),
            password = pdu.password.toString('ascii'),
            params;

        params = { smppUser: system_id, smppPass: password };
        log.info("Client bind attempt: " + system_id + "(" + client.socket.remoteAddress + "); pid: " + process.pid);

        remote.jsonRpc(config.smscloudHost, config.smscloudPath, 'internal.smppAuth', params, function(data) {
            if (data.result != false) {
                client.api_key = data.result;
                callback("ESME_ROK");
            } else {
                callback("ESME_RBINDFAIL");
            }
        });
        return;
    });

    shortyServer.on('bindSuccess', function(client, pdu) {
        client.original_bind_type = client.bind_type;
        client.connection_uuid = uuid();
        process.send({
            cmd: 'bind',
            system_id: client.system_id,
            bind_type: client.bind_type,
            connection_uuid: client.connection_uuid
        });
    });

    shortyServer.on('submit_sm', function(client, pdu, callback) {
        var sender = pdu.source_addr.toString('ascii');

        var params = {
            fromNumber: sender,
            toNumber: pdu.destination_addr.toString('ascii'),
            message: pdu.short_message.toString('base64'),
            priority: 2,
            apiKey: client.api_key,
            encoding: pdu.data_coding
        };

        log.debug("MT attempt from " + client.system_id);

        if (sender == '11111111111' || sender == '') {
            params.fromNumber = '';
        }

        remote.jsonRpc(config.smscloudHost, config.smscloudPath 'shorty.send', params, function(data) {
            if (data.error == null) {
                if (data.result && data.result.sms_id) {
                    callback("ESME_ROK", data.result.sms_id);
                } else {
                    callback("ESME_ROK", 0);
                }
            } else {
                log.warning("MT failure (" + client.system_id + "): " + data.error.message);
                callback("ESME_RSUBMITFAIL", 0);
            }
        });
    });

    shortyServer.on('disconnect', function(client) {
        process.send({
            cmd: 'disconnect',
            system_id: client.system_id,
            bind_type: client.original_bind_type,
            connection_uuid: client.connection_uuid
        });
    });

    shortyServer.on('deliver_sm_resp', function(pdu) {
        log.info("sms delivered: " + pdu.sequence_number);
    });

    shortyServer.start();

    var tryShutdown = function() {
        if (remote.getRequestsInProgress(config.smscloudHost, 80) === 0) {
            process.exit(0);
        } else {
            setTimeout(tryShutdown, 100);
        }
    };

    var sighandle = function() {
        shortyServer.stop();

        setTimeout(process.exit, 7500, 0);
        process.nextTick(tryShutdown);
    };

    process.on('exit', function() {
        log.debug('worker ' + process.pid + ' exiting');
    });

    process.on('SIGHUP', sighandle);
    process.on('SIGINT', sighandle);
    process.on('SIGQUIT', sighandle);
    process.on('SIGTERM', sighandle);
    process.on('SIGSTOP', sighandle);
}
