assert   = require 'assert'
JobQueue = require '../lib/jobqueue'


createJobQueue = ({ keys }) ->
  queue = new JobQueue()
  queue.log = []

  stringifyRequest = (request) -> (" #{key}=#{request[key]}" for key in keys when request[key]).join('')

  queue.on 'running', (id, request) ->
    queue.log.push "running #{id}#{stringifyRequest request}"
  queue.on 'complete', (id, request) ->
    queue.log.push "complete #{id}#{stringifyRequest request}"
  queue.on 'drain', ->
    queue.log.push "drain"

  queue.logRequest = (kw) ->
    (request, callback) ->
      queue.log.push "#{kw}#{stringifyRequest request}"
      callback(null)

  queue.assert = (expected) ->
    assert.equal queue.log.join("\n"), expected.join("\n")

  return queue


describe "JobQueue", ->

  it "should run a simple task", (done) ->
    queue = createJobQueue(keys: ['project', 'action'])

    queue.register { action: 'foo' }, queue.logRequest('foo')

    await
      queue.once 'drain', defer()
      queue.add { project: 'woot', action: 'foo' }

    queue.assert [
      'running action:foo project=woot action=foo'
      'foo project=woot action=foo'
      'complete action:foo project=woot action=foo'

      'drain'
    ]
    done()


  it "should run two simple tasks", (done) ->
    queue = createJobQueue(keys: ['project', 'action'])

    queue.register { action: 'foo' }, queue.logRequest('foo')
    queue.register { action: 'bar' }, queue.logRequest('bar')

    await
      queue.once 'drain', defer()
      queue.add { project: 'woot', action: 'foo' }
      queue.add { project: 'woot', action: 'bar' }

    queue.assert [
      'running action:foo project=woot action=foo'
      'foo project=woot action=foo'
      'complete action:foo project=woot action=foo'

      'running action:bar project=woot action=bar'
      'bar project=woot action=bar'
      'complete action:bar project=woot action=bar'

      'drain'
    ]
    done()
