#!/usr/bin/env node

var fs      = require('fs'),
    Redback = require('redback'),
    path    = require('path'),
    shorty  = require('shorty'),
    cluster = require('cluster'),
    remote  = require('./lib/remote'),
    uuid    = require('node-uuid'),
    http    = require('http'),
    remote  = require('./lib/remote'),
    args    = require('optimist').default('daemon', true).default('dev', false).argv,
    Log     = require('log'),
    daemon  = require('daemon'),
    fsext   = require('fs-ext'),
    configFile = "",
    queueList = {},
    workers = [],
    env = "",
    redback, redbackPubSub, log, smpp;

shorty.addVendorCommandStatus({
    ESME_TEMP_APP_ERROR: { value: 0x400, description: "Temporary application error" }
});

smpp = shorty.getSmppDefinitions();

if (args.dev) {
    configFile = 'transmitter.dev.json';
    env = 'dev';
} else {
    configFile = 'transmitter.json';
    env = 'production';
}

// load the config file
try {
    var config = JSON.parse(fs.readFileSync(configFile).toString());
} catch (err) {
    configFile = path.join(path.dirname(process.argv[1]), configFile);
    var config = JSON.parse(fs.readFileSync(configFile).toString());
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

// Try to setuid
//if (env === 'production') {
//    try {
//        process.setuid(config.runAsUid);
//    } catch (err) {
//        console.log('Please run as root');
//        process.exit(1);
//    }
//}

if (!args.daemon) {
    log = new Log(Log.DEBUG);
} else {
    log = new Log(Log.INFO, fs.createWriteStream(config.outFile, {flags: 'a'}));
}

redback = Redback.createClient();
redback.addStructure('SimpleQueue', {
    add: function(value, callback) {
        this.client.lpush('mt:queue:' + this.key, value, callback);
    },
    next: function(callback) {
        this.client.lpop('mt:queue:' + this.key, callback);
    },
    unpop: function(value, callback) {
        this.client.rpush('mt:queue:' + this.key, value, callback);
    }
});

if (cluster.isMaster) {
    var workers = [],
        numWorkers = require('os').cpus().length,
        deadWorkers = 0;

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

    var processQueue = function(queueName) {
        queueList[queueName].queue.next(function(err, response) {
            if (response !== null) {
                var message;
                try {
                    message = JSON.parse(response);
                } catch (ex) {} // bad JSON

                sendMessage(message, queueName);

                queueList[queueName].timer = setTimeout(processQueue, queueList[queueName].interval, queueName);
            } else {
                queueList[queueName].timer = null;
            }
        });
    };

    var updateQueue = function(queue, interval, update) {
        if (queueList[queue] === undefined) {
            queueList[queue] = {
                queue: redback.createSimpleQueue(queue)
            };
        }

        if (queueList[queue].timer !== undefined && queueList[queue].timer !== null) {
            clearTimeout(queueList[queue].timer);
        }

        queueList[queue].interval = interval;
        queueList[queue].timer = setTimeout(processQueue, interval, queue);

        if (update !== undefined && update === true) {
            redback.client.set('mt:interval:' + queue, interval);
        }
    };

    var sendMessage = function(message, queueName) {
        var workerIndex, buffer;

        if (workers.length == 0) {
            queueList[queueName].queue.unpop(JSON.stringify(message), function(err, resp) {});
        }

        command = {
            command: "send",
            body: message
        };

        worker = workers.pop();
        worker.send(command);
        workers.unshift(worker);
    };

    var handleCommand = function(command) {
        var cmd = command.command,
            body = command.body;

        switch(cmd) {
            case 'send':
                updateQueue(body.did, body.interval, true);
                break;
        }
    };

    redbackPubSub = Redback.createClient();
    var channel = redbackPubSub.createChannel('sms-mt').subscribe();
    channel.on('message', function(msg) {
        try {
            handleCommand(JSON.parse(msg));
        } catch (ex) {
            log.error("got bad JSON from redis: " + msg);
        }
    });

    var loadExistingQueueIntervals = function() {
        redback.client.keys('mt:interval:*', function(err, response) {
            for (var i in response) {
                (function(){
                    var index = i;
                    redback.client.get(response[index], function(err, resp) {
                        updateQueue(response[index].split(':')[2], resp, false);
                    });
                })();
            }
        });
    };

    var loadOtherExistingQueues = function() {
        redback.client.keys('mt:queue:*', function(err, response) {
            for (var i in response) {
                var name = response[i].split(':')[2];
                if (queueList[name] === undefined) {
                    updateQueue(name, 2000, false);
                }
            }
        });
    };

    for (var i = 0; i < numWorkers; i++) {
        workers.push(cluster.fork());
    }

    setTimeout(loadExistingQueueIntervals, 5000);
    setTimeout(loadOtherExistingQueues, 5000);

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
    /**
     * Slave code!
     */

    process.on('message', function(msg) {
        if (msg.cmd !== undefined && msg.cmd === 'rotate-logs') {
            if (args.daemon) {
                log = new Log(Log.INFO, fs.createWriteStream(config.logPath, {flags: 'a'}));
            } else {
                log = new Log(Log.DEBUG);
            }
        } else {
            sendMessage(msg.body);
        }
    });

    var messageIds = {},
        inProgress = {};

    var sendMessage = function(message) {
        if (message.message === undefined || message.message === null) {
            log.error('bad message: ' + message.id);
            return;
        }

        if (shortyClient.bind_type !== 0 && connected !== false) {
            var id, message, encoding;

            if (message.encoding !== undefined) {
                encoding = message.encoding;
            } else {
                encoding = 0x03;
            }

            body = new Buffer(message.message, 'base64');
            id = shortyClient.sendMessage({
                source_addr: message.from.replace('+',''),
                destination_addr: message.to.replace('+',''),
                sm_length: body.length,
                data_coding: encoding,
                short_message: body
            });

            if (id === false) {
                // TODO message failed for some reason
            } else {
                inProgress[id] = message;
                log.info(message.id + ': sent to server (' + id + ')');
            }
        } else {
            var queue = redback.createSimpleQueue(message.queue);
            queue.unpop(JSON.stringify(message), function(err, resp) {});
        }
    }

    var markSent = function(pdu) {
        var messageId = inProgress[pdu.sequence_number].id;
        if (pdu.command_status == smpp.command_status.ESME_ROK.value) {
            var post = {
                'messageId': messageId,
                'smscId': pdu.message_id.toString('ascii')
            };

            remote.jsonRpc(config.smscloudHost, config.smscloudPath, 'internal.markAsSent', post, function(data) {
                delete inProgress[pdu.sequence_number];
            });
        } else if (pdu.command_status == smpp.command_status.ESME_TEMP_APP_ERROR.value) {
            log.emergency("ESME_TEMP_APP_ERROR from SMSC");

            // put the message back in the queue to try again
            var queue = redback.createSimpleQueue(inProgress[pdu.sequence_number].queue);
            queue.unpop(JSON.stringify(inProgress[pdu.sequence_number]));
            delete inProgress[pdu.sequence_number];
        } else {
            var post = {
                messageId: messageId
            };

            remote.jsonRpc(config.smscloudHost, config.smscloudPath, 'internal.markAsError', post, function(data) {
                log.error(messageId + ': error sending (' + smpp.command_status_codes[pdu.command_status] + ')');
                delete inProgress[pdu.sequence_number];
            });
        }
    };

    shortyClient = shorty.createClient(config.client);
    shortyClient.on('submit_sm_resp', markSent);

    shortyClient.on('bindSuccess', function(id) {
        connected = true;
    });

    shortyClient.on('bindFailure', function() {
        connected = false;
        log.alert('bind failure');
    });

    shortyClient.on('disconnect', function() {
        connected = false;
        log.alert('disconnect');
    });

    shortyClient.connect();

    var tryShutdown = function() {
        if (remote.getRequestsInProgress(config.smscloudHost, '80') === 0) {
            process.exit(0);
        } else {
            setTimeout(tryShutdown, 100);
        }
    };

    var sighandle = function() {
        shortyClient.shouldReconnect = false;
        shortyClient.unbind();

        setTimeout(process.exit, 5000, 0);
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
