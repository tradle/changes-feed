var _tape = require('tape')
var mem = require('memdb')
var collect = require('stream-collector')
var _changes = require('./')

function memdb (opts) {
  opts = opts || {}
  opts.valueEncoding = opts.valueEncoding || 'binary'
  return mem(opts)
}

const initVariants = [null, { start: 0 }]
initVariants.forEach(runTests)

function runTests (feedOpts) {
  var changes = function (db) {
    return _changes(db, feedOpts)
  }

  var tape = function (name, test) {
    name = name + ' (' + (feedOpts ? 'custom' : 'default') + ')'
    return _tape(name, test)
  }

  tape('append and stream', function(t) {
    var feed = changes(memdb())

    feed.append('hello', function(err, node) {
      t.notOk(err, 'no err')
      t.same(node, {change:feed.start, value:new Buffer('hello')})
      collect(feed.createReadStream(), function(err, changes) {
        t.notOk(err, 'no err')
        t.same(changes.length, 1, '1 change')
        t.same(changes[0], {change:feed.start, value:new Buffer('hello')})
        t.end()
      })
    })
  })

  tape('append twice and stream', function(t) {
    var feed = changes(memdb())

    feed.append('hello', function() {
      feed.append('world', function() {
        collect(feed.createReadStream(), function(err, changes) {
          t.notOk(err, 'no err')
          t.same(changes.length, 2, '2 changes')
          t.same(changes[0], {change:feed.start, value:new Buffer('hello')})
          t.same(changes[1], {change:feed.start + 1, value:new Buffer('world')})
          t.end()
        })
      })
    })
  })

  tape('append twice and stream (2)', function(t) {
    var feed = changes(memdb())

    feed.append('hello', function () {
      process.nextTick(function () {
        t.equal(feed.batch.length, 2)
      })

      feed.append('hello again')
      feed.append('world', function() {
        collect(feed.createReadStream(), function(err, changes) {
          t.notOk(err, 'no err')
          t.same(changes.length, 3, '2 changes')
          t.same(changes[0], {change:feed.start, value:new Buffer('hello')})
          t.same(changes[1], {change:feed.start + 1, value:new Buffer('hello again')})
          t.same(changes[2], {change:feed.start + 2, value:new Buffer('world')})
          t.end()
        })
      })
    })
  })

  tape('append and live stream', function(t) {
    var feed = changes(memdb())

    feed.createReadStream({live:true})
      .on('data', function(data) {
        t.same(data, {change:feed.start, value:new Buffer('hello')})
        t.end()
      })

    setImmediate(function() {
      feed.append('hello')
    })
  })

  tape('append close and reopen', function(t) {
    var db = memdb()
    var feed = changes(db)

    feed.append('hello', function() {
      var feed2 = changes(db)
      feed.append('world', function(err, node) {
        t.same(node, {change:feed.start + 1, value:new Buffer('world')})
        t.end()
      })
    })
  })

  tape('reverse', function(t) {
    var feed = changes(memdb())

    feed.append('hello', function() {
      feed.append('world', function() {
        collect(feed.createReadStream({ reverse: true }), function(err, changes) {
          t.notOk(err, 'no err')
          t.same(changes.length, 2, '2 changes')
          t.same(changes[0], {change:feed.start + 1, value:new Buffer('world')})
          t.same(changes[1], {change:feed.start, value:new Buffer('hello')})
          t.end()
        })
      })
    })
  })

  tape('limit', function(t) {
    var feed = changes(memdb())

    feed.append('hello', function() {
      feed.append('world', function() {
        collect(feed.createReadStream({ limit: 1 }), function(err, changes) {
          t.notOk(err, 'no err')
          t.same(changes.length, 1, 'limited to 1 change')
          t.same(changes[0], {change:feed.start, value:new Buffer('hello')})
          t.end()
        })
      })
    })
  })

  tape('count', function(t) {
    var feed = changes(memdb())
    feed.append('hello', function() {
      feed.append('world', function() {
        feed.count(function(err, count) {
          t.notOk(err, 'no err')
          t.same(count, 2)
          t.end()
        })
      })
    })
  })

  tape('keys/values only', function(t) {
    t.plan(10)

    var put = ['hello', 'world']
    var feed = changes(memdb())
    var lastChange = feed.start
    var lastValIdx = 0

    feed.createReadStream({ live: true, limit: 1, keys: false })
      .on('error', t.error)
      .on('data', function (change) {
        t.same(change, new Buffer(put[lastValIdx++]))
      })

    feed.createReadStream({ live: true, limit: 1, values: false })
      .on('error', t.error)
      .on('data', function (change) {
        t.same(change, lastChange++)
      })

    feed.append(put[0], function() {
      feed.append(put[1], function() {
        collect(feed.createReadStream({ limit: 1, keys: false }), function (err, changes) {
          t.notOk(err, 'no err')
          t.same(changes.length, 1, 'limited to 1 change')
          t.same(changes[0], new Buffer(put[0]))
        })

        collect(feed.createReadStream({ limit: 1, values: false }), function (err, changes) {
          t.notOk(err, 'no err')
          t.same(changes.length, 1, 'limited to 1 change')
          t.same(changes[0], feed.start)
        })
      })
    })
  })

  tape('json valueEncoding', function(t) {
    var feed = changes(memdb({ valueEncoding: 'json' }))
    var data = { hello: 'world' }

    feed.append(data, function() {
      collect(feed.createReadStream({ limit: 1 }), function(err, changes) {
        t.notOk(err, 'no err')
        t.same(changes.length, 1, 'limited to 1 change')
        t.same(changes[0], { change:feed.start, value: data })
        t.end()
      })
    })
  })

  tape('get()', function(t) {
    var feed = changes(memdb({ valueEncoding: 'json' }))
    var data = { hello: 'world' }

    feed.append(data, function() {
      feed.get(feed.start, function(err, value) {
        t.notOk(err, 'no err')
        t.same(value, data)
        t.end()
      })
    })
  })
}
