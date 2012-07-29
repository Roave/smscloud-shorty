var http    = require('http'),
    https   = require('https'),
    qs      = require('querystring');

exports.setMaxSockets = function(max) {
    http.globalAgent.maxSockets = max;
};

exports.getRequestsInProgress = function(host, port) {
    var key = host + ':' + port,
        module = (port == '443') ? https : http;

    if (module.globalAgent.requests[key]) {
        return module.globalAgent.requests[host + ':' + port].length;
    } else {
        return 0;
    }
};

exports.jsonRpc = function(host, path, method, params, callback) {
    var postData = JSON.stringify({
        'method': method,
        'params': params,
        'id': Math.floor(Math.random() * 1001)
    });
    
    var options = {
        host: host,
        path: path,
        port: 80,
        method: 'POST',
        headers: {
            'Host': host,
            'Content-Length': postData.length
        }
    };
    
    var jsonClient = http.request(options, function(response) {
        response.setEncoding('utf8');
        response.content = '';
        response.on('data', function(chunk) {
            response.content += chunk;
        });
        response.on('end', function() {
            try {
                jsonResponse = JSON.parse(response.content);
            } catch(err) {
                console.log('EXCEPTION '+new Date().getTime()+': '+response.content);
                return;
            }
            callback(jsonResponse);
        });
    });
    
    jsonClient.on('error', function(e) {
        callback(false);
    });

    jsonClient.write(postData);
    jsonClient.end();
};

exports.post = function(host, port, path, params, secure, callbackParam, callback) {
    var postData = qs.stringify(params);

    var options = {
        host: host,
        path: path,
        port: port,
        method: 'POST',
        headers: {
            'Content-Length': postData.length,
            'Content-Type': 'application/x-www-form-urlencoded'
        }
    };

    if (secure) {
        module = https;
    } else {
        module = http;
    }

    var client = module.request(options, function(response) {
        response.setEncoding('utf8');
        response.content = '';
        response.on('data', function(chunk) {
            response.content += chunk;
        });

        response.on('end', function() {
            callback(response.content, callbackParam);
        });
    });

    client.on('error', function(e) {
        callback(false, callbackParam);
    });

    client.write(postData, 'utf8');
    client.end();
};
