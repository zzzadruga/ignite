/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

var Server = require("./server").Server;
var SqlFieldsQuery = require("./sql-fields-query").SqlFieldsQuery
var SqlQuery = require("./sql-query").SqlQuery

/**
 * Creates an instance of Cache
 *
 * @constructor
 * @this {Cache}
 * @param {Server} server Server class
 * @param {string} cacheName Cache name
 */
function Cache(server, cacheName) {
    this._server = server;
    this._cacheName = cacheName;
    this._cacheNameParam = Server.pair("cacheName", this._cacheName);
}

/**
 * Get cache value
 *
 * @this {Cache}
 * @param {string} key Key
 * @param {onGet} callback Called on finish
 */
Cache.prototype.get = function(key, callback) {
    this._server.runCommand("get", [this._cacheNameParam, Server.pair("key", key)], callback);
};

/**
 * Put cache value
 *
 * @this {Cache}
 * @param {string} key Key
 * @param {string} value Value
 * @param {noValue} callback Called on finish
 */
Cache.prototype.put = function(key, value, callback) {
    this._server.runCommand("put", [this._cacheNameParam, Server.pair("key", key), Server.pair("val", value)],
        callback);
}

/**
 * Remove cache key
 *
 * @this {Cache}
 * @param {string} key Key
 * @param {noValue} callback Called on finish
 */
Cache.prototype.remove = function(key, callback) {
    this._server.runCommand("rmv", [this._cacheNameParam, Server.pair("key", key)], callback);
}

/**
 * Remove cache keys
 *
 * @this {Cache}
 * @param {string[]} keys Keys to remove
 * @param {noValue} callback Called on finish
 */
Cache.prototype.removeAll = function(keys, callback) {
    var params = [this._cacheNameParam];

    params = params.concat(Cache.concatParams("k", keys));

    this._server.runCommand("rmvall", params, callback);
}

/**
 * Put keys to cache
 *
 * @this {Cache}
 * @param {Object.<string, string>} map collection of entries to put in the cache
 * @param {noValue} callback Called on finish
 */
Cache.prototype.putAll = function(map, callback) {
    var keys = Object.keys(map);

    var values = [];

    for (var key of keys) {
        values.push(map[key]);
    }

    var params = Cache.concatParams("k", keys);
    params = params.concat(Cache.concatParams("v", values));

    params.push(this._cacheNameParam);

    this._server.runCommand("putall", params, callback);
}

/**
 * Callback for cache get
 *
 * @callback Cache~onGetAll
 * @param {string} error Error
 * @param {string[]} results Result values
 */

/**
 * Get keys from the cache
 *
 * @this {Cache}
 * @param {string[]} keys Keys
 * @param {Cache~onGetAll} callback Called on finish
 */
Cache.prototype.getAll = function(keys, callback) {
    var params = Cache.concatParams("k", keys);

    params.push(this._cacheNameParam);

    this._server.runCommand("getall", params, callback);
}

/**
 * Execute sql query
 *
 * @param {SqlQuery} qry Query
 */
Cache.prototype.query = function(qry) {
    function onQueryExecute(qry, error, res) {
        if (error !== null) {
            qry.error(error);
            qry.end();

            return;
        }
        console.log("Qry: " + qry.type());

        console.log("Error: " + error);
        console.log("Result: " + res);

        qry.page(res["items"]);

        if (res["last"]) {
            qry.end();
        }
        else {
            this._server.runCommand("qryfetch", [
                Server.pair("cacheName", this._cacheName),
                Server.pair("qryId", res.queryId),
                Server.pair("psz", qry.pageSize())],
                onQueryExecute.bind(this, qry));
        }
    }

    if (qry.type() === "Sql") {
        this._sqlQuery(qry, onQueryExecute);
    }
    else {
        this._sqlFieldsQuery(qry, onQueryExecute);
    }
}

Cache.prototype._sqlFieldsQuery = function(qry, onQueryExecute) {
    var params = [Server.pair("cacheName", this._cacheName),
        Server.pair("qry", qry.query()),
        Server.pair("psz", qry.pageSize())];

    params = params.concat(this._sqlArguments(qry.arguments()));

    this._server.runCommand("qryfieldsexecute", params,
        onQueryExecute.bind(this, qry));
}

Cache.prototype._sqlArguments = function(args) {
    var res = [];
    console.log("ARGS=" + args);

    for (var i = 1; i <= args.length; i++) {
        res.push(Server.pair("arg" + i, args[i - 1]));
    }

    return res;
}

Cache.prototype._sqlQuery = function(qry, onQueryExecute) {
    var params = [Server.pair("cacheName", this._cacheName),
        Server.pair("qry", qry.query()),
        Server.pair("psz", qry.pageSize())]

    params = params.concat(this._sqlArguments(qry.arguments()));

    if (qry.returnType() != null) {
        params.push(Server.pair("type", qry.returnType()));
    }
    else {
        qry.error("No type for sql query.");
        qry.end();

        return;
    }

    this._server.runCommand("qryexecute", params,
        onQueryExecute.bind(this, qry));
}

/**
 * Concatenate all parameters
 *
 * @param {string} pref Prefix
 * @param {string[]} keys Keys
 * @returns List of parameters.
 */
Cache.concatParams = function(pref, keys) {
    var temp = []

    for (var i = 1; i <= keys.length; ++i) {
        temp.push(Server.pair(pref + i, keys[i-1]));
    }

    return temp;
}

exports.Cache = Cache