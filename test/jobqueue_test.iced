assert   = require 'assert'
JobQueue = require '../lib/jobqueue'


# Creates a job queue set up for convenient testing.
createJobQueue = ({ keys, logRunning, logComplete }) ->
  queue = new JobQueue()
  queue.log = []

  stringifyRequest = (request) -> ("#{key}:#{JobQueue.stringifyValue request[key]}" for key in keys when request[key]).join('-')

  if logRunning
    queue.on 'running', (job) ->
      queue.log.push "running #{job.handler.id} #{stringifyRequest job.request}"
  if logComplete
    queue.on 'complete', (job) ->
      queue.log.push "complete #{job.handler.id} #{stringifyRequest job.request}"

  queue.logRequest = (kw) ->
    (request, callback) ->
      queue.log.push "#{kw} #{stringifyRequest request}"
      callback(null)

  queue.assert = (expected) ->
    assert.equal queue.log.join("\n"), expected.join("\n")

  return queue


describe "JobQueue", ->

  describe '#add', ->

    it "should add and execute a single task", (done) ->
      queue = createJobQueue(keys: ['project', 'action'], logRunning: yes, logComplete: yes)

      queue.register { action: 'foo' }, queue.logRequest('foo')

      await
        queue.once 'drain', defer()
        queue.add { project: 'woot', action: 'foo' }

      queue.assert [
        'running action:foo project:woot-action:foo'
        'foo project:woot-action:foo'
        'complete action:foo project:woot-action:foo'
      ]
      done()


    it "should add and execute two tasks serially", (done) ->
      queue = createJobQueue(keys: ['project', 'action'], logRunning: yes, logComplete: yes)

      queue.register { action: 'foo' }, queue.logRequest('foo')
      queue.register { action: 'bar' }, queue.logRequest('bar')

      await
        queue.once 'drain', defer()
        queue.add { project: 'woot', action: 'foo' }
        queue.add { project: 'woot', action: 'bar' }

      queue.assert [
        'running action:foo project:woot-action:foo'
        'foo project:woot-action:foo'
        'complete action:foo project:woot-action:foo'

        'running action:bar project:woot-action:bar'
        'bar project:woot-action:bar'
        'complete action:bar project:woot-action:bar'
      ]
      done()


    it "should emit an error when adding a task that does not match any handlers", ->
      queue = createJobQueue(keys: ['project', 'action'])

      queue.register { action: 'foo' }, queue.logRequest('foo')

      assert.throws ->
        queue.add { project: 'woot', action: 'bar' }
      , /No handlers match/i


    it "should merge two tasks with the same id", (done) ->
      queue = createJobQueue(keys: ['project', 'action'])

      queue.register { action: 'foo' }, queue.logRequest('foo')

      await
        queue.once 'drain', defer()
        queue.add { project: ['woot'], action: 'foo' }
        queue.add { project: ['cute'], action: 'foo' }

      queue.assert [
        "foo project:[ 'woot', 'cute' ]-action:foo"
      ]
      done()


    it "should merge tasks with a custom merge handler when one is provided", (done) ->
      queue = createJobQueue(keys: ['action', 'flag', 'files'])

      merge = (a, b) ->
        a.flag ||= b.flag
        a.files.splice(0, 0, b.files...)
      queue.register { action: 'foo' }, { merge }, queue.logRequest('foo')

      await
        queue.once 'drain', defer()
        queue.add { action: 'foo', flag: yes, files: ['x.txt'] }
        queue.add { action: 'foo', flag: no,  files: ['y.txt'] }

      queue.assert [
        "foo action:foo-flag:true-files:[ 'x.txt', 'y.txt' ]"
      ]
      done()


    it "should not merge tasks that have different ids because of custom idKeys", (done) ->
      idKeys = ['project', 'action']
      queue  = createJobQueue(keys: idKeys)

      queue.register { action: 'foo' }, { idKeys }, queue.logRequest('foo')

      await
        queue.once 'drain', defer()
        queue.add { project: 'woot', action: 'foo' }
        queue.add { project: 'cute', action: 'foo' }

      queue.assert [
        "foo project:woot-action:foo"
        "foo project:cute-action:foo"
      ]
      done()


  describe "#checkDrain", ->

    it "should emit 'drain' if no tasks have been scheduled", (done) ->
      queue = createJobQueue(keys: ['project', 'action'])
      queue.register { action: 'foo' }, queue.logRequest('foo')

      await
        queue.once 'drain', defer()
        queue.checkDrain()

      queue.assert []
      done()

    it "shouldn't emit 'drain' if there's a scheduled task", (done) ->
      queue = createJobQueue(keys: ['action'])
      queue.register { action: 'foo' }, queue.logRequest('foo')

      await
        queue.add { action: 'foo' }

        cb = defer()
        queue.once 'drain', ->
          queue.log.push 'drain'
          cb()
        queue.checkDrain()
        queue.log.push 'not yet'

      queue.assert [
        "not yet"
        "foo action:foo"
        "drain"
      ]
      done()

    it "shouldn't emit 'drain' if there's a running task", (done) ->
      queue = createJobQueue(keys: ['action'])
      queue.register { action: 'foo' }, (request, done) ->
        queue.log.push 'foo:start'
        queue.checkDrain()
        queue.log.push 'foo:end'
        done()

      await
        queue.add { action: 'foo' }

        cb = defer()
        queue.once 'drain', ->
          queue.log.push 'drain'
          cb()

      queue.assert [
        "foo:start"
        "foo:end"
        "drain"
      ]
      done()



  describe '#getQueuedRequests', ->

    it "should return an empty array for an empty queue", ->
      assert.deepEqual new JobQueue().getQueuedRequests(), []

    it "should return the requests of all added tasks, in order", ->
      queue = new JobQueue()
      queue.register { action: 'foo' }, ->
      queue.register { action: 'bar' }, ->
      queue.add { project: 'woot', action: 'foo' }
      queue.add { project: 'cute', action: 'bar' }

      assert.deepEqual queue.getQueuedRequests(), [{ project: 'woot', action: 'foo' }, { project: 'cute', action: 'bar' }]


  describe "'empty' event", (done) ->

    it "should be emitted when the queue gets drained, after the drain event", ->
      queue = createJobQueue(keys: ['action'])
      queue.register { action: 'foo' }, queue.logRequest('foo')
      queue.add { action: 'foo' }
      queue.on 'drain', -> queue.log.push 'drain'
      queue.on 'empty', -> queue.log.push 'empty'

      await queue.once 'empty', defer()
      queue.assert [
        "foo action:foo"
        "drain"
        "empty"
      ]

    it "should be emitted once at the very end if more jobs get added by 'drain' handlers", ->
      queue = createJobQueue(keys: ['action'])
      queue.register { action: 'foo' }, queue.logRequest('foo')
      queue.add { action: 'foo' }
      queue.on 'drain', -> queue.log.push 'drain'
      queue.on 'empty', -> queue.log.push 'empty'

      queue.once 'drain', ->
        queue.add { action: 'foo' }

      await queue.once 'empty', defer()
      queue.assert [
        "foo action:foo"
        "drain"
        "foo action:foo"
        "drain"
        "empty"
      ]
