// middleware and helpers
var koa 	= require('koa'),
	serve 	= require('koa-static'),
	parse 	= require('co-body'),
	router 	= require('koa-router'),
	http 	= require('http');

var route = require('koa-route');

// import rethinkdb
var r = require('rethinkdb');

// load config for rethinkdb and koa
var config = require(__dirname + "/config.js");

// create an http server with koa
var app = koa();

// serve static content
app.use(serve(__dirname + '/public'));

// create a rethinkdb connection
app.use(createConnection);

// routes
// app.use(router(app));
// app.get('/todo/get', get);
// app.put('/todo/new', create);
// app.post('/todo/update', update);
// app.post('/todo/delete', del);

// close the rethinkdb connection
app.use(closeConnection);

// function to create a rethinkdb connection
function* createConnection(next) {
	try {
		// open a connection and 
		// wait for r.connect to be resolved
		var conn = yield r.connect(config.rethinkdb);

		// save the connection in the current context 
		// (will be passed on to the next middleware)

		this._rdbConn = conn;

	}
	catch (err) {
		this.status = 500;
		this.body = err.message || http.STATUS_CODES[this.status];
	}
	yield next;
}

// retrieve all the todos
function * create(next) {
	try {
		var cursor = yield r.table('todos')
			.orderBy({index: "createdAt"})
			.run(this._rdbConn);

		var result = yield cursor.toArray();
		this.body = JSON.stringify(result);
	}
	catch(e) {
		this.status = 500;
		this.body = e.message || http.STATUS_CODES[this.status];
	}
	yield next;
}

// insert a todo
function * create(next) {
	try {
		// parse the POST data
		var todo = yield parse(this);
		// set the field createdAt to the current time
		todo.createdAt = r.now(); 

		// insert a new todo
		var result = yield r.table('todos')
			.insert(todo, {returnChanges: true})
			.run(this._rdbConn);

		// todo now contains the previous todo + 
		// a field id and createdAt
		todo = result.new_val;

		this.body = JSON.stringify(todo);
	}	

	catch(e) {
		this.status = 500;
		this.body = e.message || http.STATUS_CODES[this.status];
	}

	yield next;
}

// update a todo
function * update(next) {
	try {
		var todo = yield parse(this);
		delete todo._saving;

		if((todo == null) || (todo.id == null)) {
			throw new Error("The todo must have a field id.");
		}

		var result = yield r.table('todos')
			.get(todo.id)
			.update(todo, {returnChanges: true})
			.run(this._rdbConn);

		this.body = JSON.stringify(result.changes[0].new_val);
	}
	catch(e) {
		this.status = 500;
		this.body = e.message || http.STATUS_CODES[this.status];
	}
	yield next;
}

// delete a todo
function * del(next) {
	try {
		var todo = yield parse(this);

		if((todo == null) || (todo.id == null)) {
			throw new Error("The todo must have a field id");
		}

		var result = yield r.table('todos')
			.get(todo.id)
			.delete()
			.run(this._rdbConn);

		this.body = "";
	}
	catch(e) {
		this.status = 500;
		this.body = e.message || http.STATUS_CODES[this.status];
	}
	yield next;
}

function * closeConnection(next) {
	this._rdbConn.close();
}

r.connect(config.rethinkdb, function(err, conn) {
	if(err) {
		console.log("Could not open a connection to initialize the database");
		console.log(err.message);
		process.exit(1);
	}

	r.table('todos')
		.indexWait('createdAt')
		.run(conn)
		.then(function(err, result) {
			console.log("Table and index are available, starting koa...");
			startKoa();
		})
		.error(function(err) {
			// the database/table/index was not available, create them
			r.dbCreate(config.rethinkdb.db).run(conn).finally(function() {
					return r.tableCreate('todos').run(conn)
				}).finally(function() {
					r.table('todos').indexCreate('createdAt').run(conn);
				}).finally(function(result) {
					r.table('todos').indexWait('createdAt').run(conn)
				}).then(function(result) {
					console.log("Table and index are available, starting koa...");
					startKoa();
					conn.close();
				}).error(function(err) {
					if(err) {
						console.log("Could not wait for the completion of the index todos.");
						console.log(err);
						process.exit(1);
					}
					console.log("Table and index are available, starting koa...");
					startKoa();
					conn.close();
				});
		}); 
});