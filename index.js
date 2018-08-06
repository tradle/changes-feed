var EventEmitter = require('events').EventEmitter
var lexint = require('lexicographic-integer')
var collect = require('stream-collector')
var through = require('through2')
var from = require('from2')
var pump = require('pump')
var thunky = require('thunky')

var noop = function() {}
var defaultStart = 1

module.exports = function(db, feedOpts) {
  var start = defaultStart
  if (feedOpts && typeof feedOpts.start === 'number') {
    start = feedOpts.start
  }

  var feed = {}
  var valueEncoding = db.options.valueEncoding || 'binary'
  var ready
  var ensureCount = thunky(function(cb) {
    collect(db.createKeyStream({reverse:true, limit:1}), function(err, keys) {
      if (err) return cb(err)
      if (!keys.length) return cb()
      feed.change = lexint.unpack(keys[0], 'hex')
      ready = true
      feed.tentativeChange = feed.change + queuedBeforeReady
      cb()
    })
  })

  var queuedBeforeReady = 0
  feed.start = start
  feed.change = start - 1
  feed.tentativeChange = feed.change
  feed.queued = 0
  feed.notify = []
  feed.batch = []
  feed.onready = ensureCount
  feed.count = function(cb) {
    ensureCount(function (err) {
      if (err) return cb(err)
      cb(null, feed.change - start + 1)
    });
  }

  feed.append = function(value, cb) {
    feed.queued++
    feed.tentativeChange++
    if (!ready) queuedBeforeReady++

    if (!cb) cb = noop
    if (valueEncoding === 'binary' && !Buffer.isBuffer(value)) {
      value = new Buffer(value)
    }

    ensureCount(function(err) {
      if (err) return release(cb, err)

      var batch = feed.batch
      var change = ++feed.change
      batch.push({
        change: change,
        key: lexint.pack(change, 'hex'),
        value: value,
        callback: cb
      })

      if (batch.length !== 1) return

      // schedule batch commit
      process.nextTick(function () {
        batch = batch.slice()
        feed.batch.length = 0

        db.batch(batch.map(function (item) {
          return {
            type: 'put',
            key: item.key,
            value: item.value
          }
        }), function(err) {
          feed.queued = 0
          var notify = feed.notify

          if (notify.length) {
            feed.notify = []
            for (var i = 0; i < notify.length; i++) notify[i][0](1, notify[i][1])
          }

          batch.forEach(function (item, i) {
            item.callback(err, { change: batch[i].change, value: value })
          })
        })
      })
    })
  }

  feed.createReadStream = function (opts) {
    if (typeof opts === 'function') return feed.createReadStream(null, opts)
    if (!opts) opts = {}

    var since = typeof opts.since === 'number' ? opts.since : start - 1
    var keys = opts.keys !== false
    var values = opts.values !== false
    var retOpts = {
      keys: keys,
      values: values
    }

    if (opts.live) {
      var ls = from.obj(function read(size, cb) {
        feed.get(since + 1, function(err, value) {
          if (err && err.notFound) return feed.notify.push([read, cb])
          if (err) return cb(err)
          cb(null, toResult(++since, value, retOpts))
        })
      })

      db.once('closing', destroy)
      ls.once('close', function () {
        db.removeListener('closing', destroy)
      })

      return ls

      function destroy () {
        ls.destroy()
      }
    }

    var rs = db.createReadStream({
      gt: lexint.pack(since, 'hex'),
      limit: opts.limit,
      keys: keys,
      values: values,
      reverse: opts.reverse,
      valueEncoding: valueEncoding
    })

    var format = function(data, enc, cb) {
      var key = keys && lexint.unpack(values ? data.key : data, 'hex')
      var val = values && keys ? data.value : data

      cb(null, toResult(key, val, retOpts))
    }

    return pump(rs, through.obj(format))
  }

  feed.get = function (id, opts, cb) {
    if (typeof opts === 'function') {
      cb = opts
      opts = {}
    }

    opts.valueEncoding = valueEncoding
    return db.get(lexint.pack(id, 'hex'), opts, cb)
  }

  feed.del = function (id, cb) {
    return db.del(lexint.pack(id, 'hex'), cb)
  }

  function toResult (key, val, opts) {
    return !opts.keys ? val :
      !opts.values ? key :
      { change: key, value: val }
  }

  return feed
}
