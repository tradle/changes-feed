const lexint = require('lexicographic-integer')
const collect = require('stream-collector')
const through = require('through2')
const from = require('from2')
const pump = require('pump')
const thunky = require('thunky')

const noop = function () {}
const defaultStart = 1

module.exports = function (db, feedOpts) {
  let start = defaultStart
  if (feedOpts && typeof feedOpts.start === 'number') {
    start = feedOpts.start
  }

  const feed = {}
  const valueEncoding = db.options.valueEncoding || 'binary'
  let ready
  let queuedBeforeReady = 0
  const ensureCount = thunky(function (cb) {
    collect(db.createKeyStream({ reverse: true, limit: 1 }), function (err, keys) {
      if (err) return cb(err)
      if (!keys.length) return cb()
      feed.change = lexint.unpack(keys[0], 'hex')
      ready = true
      feed.tentativeChange = feed.change + queuedBeforeReady
      cb()
    })
  })

  feed.start = start
  feed.change = start - 1
  feed.tentativeChange = feed.change
  feed.queued = 0
  feed.notify = []
  feed.batch = []
  feed.onready = ensureCount
  feed.count = function (cb) {
    ensureCount(function (err) {
      if (err) return cb(err)
      cb(null, feed.change - start + 1)
    })
  }

  feed.append = function (value, cb) {
    feed.queued++
    feed.tentativeChange++
    if (!ready) queuedBeforeReady++

    if (!cb) cb = noop
    if (valueEncoding === 'binary' && !Buffer.isBuffer(value)) {
      value = Buffer.from(value)
    }

    ensureCount(function (err) {
      if (err) return cb(err)

      let batch = feed.batch
      const change = ++feed.change
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
        }), function (err) {
          feed.queued = 0
          const notify = feed.notify

          if (notify.length) {
            feed.notify = []
            for (let i = 0; i < notify.length; i++) notify[i][0](1, notify[i][1])
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

    let since = typeof opts.since === 'number' ? opts.since : start - 1
    const keys = opts.keys !== false
    const values = opts.values !== false
    const retOpts = {
      keys: keys,
      values: values
    }

    if (opts.live) {
      const ls = from.obj(function read (size, cb) {
        feed.get(since + 1, function (err, value) {
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

    const rs = db.createReadStream({
      gt: lexint.pack(since, 'hex'),
      limit: opts.limit,
      keys: keys,
      values: values,
      reverse: opts.reverse,
      valueEncoding: valueEncoding
    })

    const format = function (data, enc, cb) {
      const key = keys && lexint.unpack(values ? data.key : data, 'hex')
      const val = values && keys ? data.value : data

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
    return !opts.keys
      ? val
      : !opts.values
          ? key
          : { change: key, value: val }
  }

  return feed
}
