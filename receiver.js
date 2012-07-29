#!/usr/bin/env node

var shorty  = require('shorty'),
    smpp    = shorty.getSmppDefinitions(),
    net     = require('net'),
    fs      = require('fs'),
    path    = require('path'),
    remote  = require('./lib/remote'),
    uuid    = require('node-uuid'),
    cluster = require('cluster'),
    daemon  = require('daemon'),
    args    = require('optimist').default('daemon', true).default('dev', false).argv,
    Log     = require('log'),
    fsext   = require('fs-ext'),
    configFile, log;

if (args.dev) {
    configFile = 'receiver.dev.json';
} else {
    configFile = 'receiver.json';
}

try {
    var config = JSON.parse(fs.readFileSync(configFile).toString());
} catch (err) {
    configFile = path.join(path.dirname(process.argv[1]), configFile);

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

if (!args.daemon) {
    log = new Log(Log.DEBUG);
} else {
    log = new Log(Log.INFO, fs.createWriteStream(config.outFile, {flags: 'a'}));
}

remote.setMaxSockets(config.max_http_requests);

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

    for (var i = 0; i < require('os').cpus().length; i++) {
        workers.push(cluster.fork());
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
    var shortyClient = shorty.createClient(config.client);
    var shortyConnected;

    process.on('message', function(msg) {
        if (msg.cmd !== undefined && msg.cmd === 'rotate-logs') {
            if (args.daemon) {
                log = new Log(Log.INFO, fs.createWriteStream(config.logPath, {flags: 'a'}));
            } else {
                log = new Log(Log.DEBUG);
            }
        }
    });

    shortyClient.on('deliver_sm', function(pdu) {
        var myUuid = uuid();

        log.info("received: " + myUuid);

        var params = {
            hostNumber: pdu.destination_addr.toString('ascii'),
            remoteNumber: pdu.source_addr.toString('ascii'),
            message: pdu.short_message.toString('base64')
        };

        remote.jsonRpc(config.smscloudHost, config.smscloudPath, 'shorty.incoming', params, function(data) {
            log.info("posted: " + myUuid);
        });
    });

    shortyClient.on('bindSuccess', function(id) {
        shortyConnected = true;
        log.info("bind success");
    });

    shortyClient.on('bindFailure', function() {
        log.info('bind failure');
    });


    shortyClient.on('disconnect', function() {
        shortyConnected = false;
        log.debug('disconnect');
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
