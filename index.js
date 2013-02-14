var util         = require('util');
var assert       = require('assert');
var EventEmitter = require('events').EventEmitter;

function Steve(namespace, options, kvsClient) {
	this.workerId  = options.name || ('steve_' + Date.now() + '_' + Math.random());
	this.namespace = namespace;
	assert.ok(typeof this.namespace === "string", "steve must have a namespace");

	this.kvs = kvsClient;
	assert.ok(typeof this.kvs === "object", "must provide a redis kvsClient");

	this._isWorking = false;
	EventEmitter.call(this);
}

util.inherits(Steve, EventEmitter);

var PushTaskScript = ""
	+ "local namespace   = KEYS[1]\n"
	+ "local taskPayload = ARGV[1]\n"
	+ "local workerId    = ARGV[2]\n"
	+ "local now         = tonumber(ARGV[3])\n"
	+ "local taskId      = redis.call('incr', namespace .. '_id_counter')\n"
	+ "local taskKey     = namespace .. '_task_' .. taskId\n"
	+ "redis.call('hmset', taskKey, 'payload', taskPayload, 'ownerId', workerId, 'state', 'wait', 'ctime', now, 'mtime', now)\n"
	+ "redis.call('lpush', namespace .. '_wait_queue', taskId)\n"
	+ "return taskId\n"


Steve.prototype.pushTask = function pushTask(task, optionalCallback) {
	var self = this;
	self.kvs.eval(PushTaskScript, 1, self.namespace, JSON.stringify(task), self.workerId, Date.now(), function(err, taskId) {
		if (optionalCallback) return optionalCallback(err, taskId)
		if (err) self.emit('error', err);
	});
}

var PullTaskScript = ""
	+ "local namespace   = KEYS[1]\n"
	+ "local workerId    = ARGV[1]\n"
	+ "local now         = tonumber(ARGV[2])\n"
	+ "local taskId      = redis.call('rpoplpush', namespace .. '_wait_queue',  namespace .. '_work_queue')\n"
	+ "if not taskId then return end\n"
	+ "local taskKey     = namespace .. '_task_' .. taskId\n"
	+ "redis.call('hmset', taskKey, 'state', 'work', 'workerId', workerId, 'mtime', now)\n"
//	+ "redis.log(redis.LOG_WARNING, 'hmset', taskKey, 'state', 'work', 'mtime', now)\n"
	+ "local task        = redis.call('hgetall', taskKey)\n"
	+ "return { taskId, task }\n"

Steve.prototype.pullTask = function pullTask(callback) {
	var self = this;
	self.kvs.eval(PullTaskScript, 1, self.namespace, self.workerId, Date.now(), function(err, ret) {
		var task;
		if (ret) {
			task = { id: parseInt(ret[0]), task: {} }
			var rawTask = ret[1];
			for( var idx = 0; idx < rawTask.length; idx += 2 ) {
				var name = rawTask[idx];
				switch(name) {
					case 'payload': task.task[name] = JSON.parse(rawTask[idx+1]); break;
					case 'ctime':   task.task[name] = new Date(parseInt(rawTask[idx+1])); break;
					case 'mtime':   task.task[name] = new Date(parseInt(rawTask[idx+1])); break;
					default:        task.task[name] = rawTask[idx+1];
				}
			}
		}
		try {
			callback(err, task, function(){});
		} catch(err2) {
			self.emit('error', err2);
		}
	});
}

var ExtendTaskScript = ""
	+ "local namespace   = KEYS[1]\n"
	+ "local taskId      = tonumber(ARGV[1])\n"
	+ "local workerId    = ARGV[2]\n"
	+ "local now         = tonumber(ARGV[3])\n"
	+ "local taskKey     = namespace .. '_task_' .. taskId\n"
	+ "local state       = redis.call('hmget', taskKey, 'state', 'workerId')\n"
//	+ "redis.log(redis.LOG_WARNING, state[1], state[2])\n"
	+ "if state[1] == 'work' and state[2] == workerId then\n"
		+ "redis.call('hmset', taskKey, 'mtime', now);\n"
	+ "else\n"
		+ "return { err = 'worker doesn\'t own task' }\n"
	+ "end"

Steve.prototype.extendTask = function extendTask(task, optionalCallback) {
	self.kvs.eval(ExtendTaskScript, 1, self.namespace, task.id, self.workerId, Date.now(), function(err) {
		if (optionalCallback) return optionalCallback(err)
		if (err) self.emit('error', err);
	});
}

var ReturnTaskScript = ""
	+ "local namespace   = KEYS[1]\n"
	+ "local taskId      = tonumber(ARGV[1])\n"
	+ "local workerId    = ARGV[2]\n"
	+ "local now         = tonumber(ARGV[3])\n"
	+ "local taskKey     = namespace .. '_task_' .. taskId\n"
	+ "local state       = redis.call('hmget', taskKey, 'state', 'workerId')\n"
//	+ "redis.log(redis.LOG_WARNING, state[1], state[2]);\n"
	+ "if state[1] == 'work' and state[2] == workerId then\n"
		+ "redis.call('hmset', taskKey, 'state', 'wait', 'mtime', now, 'workerId', '');\n"
		+ "redis.call('lrem', namespace .. '_work_queue', 0, taskId)\n"
		+ "redis.call('lpush', namespace .. '_wait_queue', taskId)\n"
	+ "else\n"
		+ "return { err = 'worker doesnt own task' }\n"
	+ "end"

Steve.prototype.returnTask = function returnTask(task, optionalCallback) {
	var self = this;
	self.kvs.eval(ReturnTaskScript, 1, self.namespace, task.id, self.workerId, Date.now(), function(err, taskId) {
	if (optionalCallback) return optionalCallback(err, taskId)
		if (err) self.emit('error', err);
	});
}

var EndTaskScript = ""
	+ "local namespace   = KEYS[1]\n"
	+ "local taskId      = tonumber(ARGV[1])\n"
	+ "local workerId    = ARGV[2]\n"
	+ "local taskKey     = namespace .. '_task_' .. taskId\n"
	+ "local state       = redis.call('hmget', taskKey, 'state', 'workerId')\n"
//	+ "redis.log(redis.LOG_WARNING, state[1], state[2]);\n"
	+ "if state[1] == 'work' and state[2] == workerId then\n"
		+ "redis.call('del', taskKey);\n"
		+ "redis.call('lrem', namespace .. '_work_queue', 0, taskId)\n"
	+ "else\n"
		+ "return { err = 'worker doesnt own task' }\n"
	+ "end"

Steve.prototype.endTask = function endTask(task, optionalCallback) {
	var self = this;
	self.kvs.eval(EndTaskScript, 1, self.namespace, task.id, self.workerId, function(err) {
		if (optionalCallback) return optionalCallback(err)
		if (err) self.emit('error', err);
	});
}

var ReclaimTaskScript = ""
	+ "local namespace   = KEYS[1]\n"
	+ "local timeout     = tonumber(ARGV[1])\n"
	+ "local now         = tonumber(ARGV[2])\n"
	+ "local list        = redis.call('lrange', namespace .. '_work_queue', 0, -1)\n"
	+ "for idx, taskId in ipairs(list) do\n"
		+ "local taskKey = namespace .. '_task_' .. taskId\n"
		+ "local mtime   = tonumber(redis.call('hget', taskKey, 'mtime')) or 0\n"
		+ "if mtime + timeout < tonumber(now) then\n"
			+ "redis.call('hmset', taskKey, 'state', 'wait', 'mtime', now, 'workerId', '');\n"
			+ "redis.call('lrem', namespace .. '_work_queue', 0, taskId)\n"
			+ "redis.call('lpush', namespace .. '_wait_queue', taskId)\n"
		+ "end\n"
	+ "end"

Steve.prototype.reclaimTasks = function reclaimTasks(timeout, optionalCallback) {
	var self = this;
	self.kvs.eval(ReclaimTaskScript, 1, self.namespace, timeout, Date.now(), function(err, taskCount) {
		if (optionalCallback) return optionalCallback(err, taskCount)
		if (err) self.emit('error', err);
	});
}

/*
var PushToErrorScript =""
	+ "local job = 
Steve.prototype.errorTask = function errorTask(job, errorStack, optionalCallback) {
	self.kvs.eval(PushToErrorScript, self.namespace + "_work_queue", self.namespace + "_error_queue", job, errorStack, function(err) { 
		if (optionalCallback) return optionalCallback(err)
		if (err) self.emit('error', err);
	});
}

Steve.prototype.pollTasks = function poll() {
	var self = this;
	self.kvs.rpoplpush(self.namespace + "_wait_queue", self.namespace + "_work_queue", function callback(err, job) {
		if (err) return self.emit("error", err);
		if (job) {
			job = JSON.parse(job);
			if (self.listeners(job.event).length > 0) {
				self._isWorking = true;
				self.emit(job.event, job, function jobCallback(err, handled) {
					if (err) {
						return self.errorTask(job, err.stack);
					}
				})
			} else {
				//this process has not registered to handle this events
				self.pushTask(job)
			}
		}
	});
}

Steve.prototype.isWorking = function isWorking() {
	return this._isWorking;
}

Steve.prototype.quit = function quit() {
	
}
*/

exports.Steve = Steve;

var redis = require("redis"),
	        client = redis.createClient();

var steve = new Steve('steve_test', {}, client);
steve.pushTask({testA:123});
setInterval(function() {
	steve.pullTask(function(err, task, callback) {
		console.log(task);
		if (task) {
		console.log(task.task.ownerId, steve.workerId, task.task.ownerId == steve.workerId);
			if (task.task.ownerId == steve.workerId) {
				steve.returnTask(task);
			} else {
				steve.endTask(task);
			}
		}
		steve.reclaimTasks(10000);
		callback();
	})
}, 5000);

