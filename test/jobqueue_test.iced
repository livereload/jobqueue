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

  it "should run a simple task", (done) ->
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


  it "should run two simple tasks", (done) ->
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


  it "should merge with a custom merge handler", (done) ->
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


  it "should allow to override the idKeys", (done) ->
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
