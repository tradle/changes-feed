const db = require('level')('db')
const feed = require('./')(db)

feed.createReadStream({ since: 2, live: true })
  .on('data', console.log)

setTimeout(function () {
  feed.append('world')
}, 400)
