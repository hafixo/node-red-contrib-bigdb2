module.exports = function(RED) {
    "use strict";
    var reconnect = RED.settings.bigdb2ReconnectTime || 30000;
    var db2 = require('ibm_db');
    var Promise = require('promise');
    var biglib = require('node-red-biglib');

    function bigdb2Node(n) {
        RED.nodes.createNode(this,n);
        this.host = n.host;
        this.port = n.port;
        this.connected = false;
        this.connecting = false;
        this.dbname = n.db;
        var node = this;

        function doConnect(conncb) {
            node.connecting = true;
            node.conn = {};
            node.connection = {
                connect: (cb) => {
                    var conStr = "DRIVER={DB2};DATABASE="+node.dbname
                                +";HOSTNAME="+node.host
                                +";UID="+node.credentials.user
                                +";PWD="+node.credentials.password
                                +";PORT="+node.port+";PROTOCOL=TCPIP"; 
                    db2.open(conStr, function (err,conn) {
                        if (err) {
                            cb(err, null);  
                        } 
                        else {
                            console.log('connection to ' + node.dbname);
                            conn.connName = node.dbname;        
                            cb(null, conn);
                        }
                    });
                },
                end: (conn) => {
                    conn.close(() => {
                        console.log('connection closed');
                    });
                }
            };
            node.connection.connect(function(err, conn) {
                node.connecting = false;
                if (err) {
                    node.error(err);
                    console.log("connection error " + err);
                } else {
                    node.conn = conn;
                    node.connected = true;
                }
                conncb(err);
            });
        }

        this.connect = function() {
            return new Promise((resolve, reject) => {
                if (!this.connected && !this.connecting) {
                    doConnect((err)=>{
                        if(err) reject(err);
                        else resolve();
                    });
                }  
                else{
                    resolve();
                }  
            });
        }

        this.on('close', function (done) {
            if (this.connection) {
                node.connection.end(this.conn);
            } 
            done();
        });
    }
    
    RED.nodes.registerType("bigdb2database", bigdb2Node, {
        credentials: {
            user: {type: "text"},
            password: {type: "password"}
        }
    });
    
    function bigdb2NodeIn(n) {
   
        RED.nodes.createNode(this,n);
        this.mydb = n.mydb;
        var node = this;

        var bignode = new biglib({ config: n, node: node, status: 'orders' });

        node.query = function(node, db, msg){
            if ( msg.payload !== null && typeof msg.payload === 'string' && msg.payload !== '') {
                db.conn.query(msg.payload, function(err, rows) {
                    if (err) { 
                        bignode._on_finish(err);
                    }
                    else {
                        rows.forEach(function(row) {
                            msg.payload = row;
                            node.send(msg);
                        })
                        bignode._on_finish();
                    }
                });
            }
            else {
                if (msg.payload === null) { 
                    bignode._on_finish(new Erro("msg.payload : the query is not defined"));
                }
                if (typeof msg.payload !== 'string') { 
                    bignode._on_finish(new Error("msg.payload : the query is not defined as a string"));
                }
                if (typeof msg.payload === 'string' && msg.payload === '') { 
                    bignode._on_finish(new Error("msg.payload : the query string is empty"));
                }
            }
        }

        node.on("input", (msg) => {
            if ( msg.database !== null && typeof msg.database === 'string' && msg.database !== '') {
                
                node.mydbNode = RED.nodes.getNode(n.mydb);
                if (node.mydbNode) {

                    var go_query = function(dbnode) {
                        bignode._on_start();
                        node.query(node, dbnode, msg);
                    }

                    //node.send([ null, { control: 'start', query: msg.payload, database: n.mydb } ]);
                    if(node.mydbNode.conn && node.mydbNode.conn.connName === msg.database){
                        console.log("already connected");
                        go_query(node.mydbNode);
                    }
                    else{
                        var findNode;
                        RED.nodes.eachNode((node)=>{
                            if(node.db && node.db === msg.database){
                                findNode = RED.nodes.getNode(node.id);
                                node.mydb = node.id;
                            }
                        })
                        findNode.connect()
                        .then(()=>{
                            go_query(findNode);
                        });
                    }
                }
                else {
                    this.error("database not configured");
                }
            }
            else{
                this.error("database not specified");
            }
        });
    }
    RED.nodes.registerType("bigdb2", bigdb2NodeIn);
}
