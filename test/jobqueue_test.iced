assert   = require 'assert'
JobQueue = require '../lib/jobqueue'

describe "JobQueue", ->

  it "should run a simple task", (done) ->
    queue = new JobQueue()

    _executed = no
    queue.register { action: 'foo' }, (done) ->
      _executed = yes

    await
      queue.on 'drain', defer()
      queue.add { project: 'woot', action: 'foo' }

    assert.ok _executed
    done()
