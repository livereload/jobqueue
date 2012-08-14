# Job queue runs a set of jobs serially, providing additional request merging and introspection
# capabilities.

# Imports
{ EventEmitter } = require 'events'

# ### JobQueue public API

# JobQueue is the class returned when you `require('jobqueue')`.
module.exports =
class JobQueue extends EventEmitter
  constructor: ->
    # Handlers registered via `#register`.
    @handlers = []

    # The list of queued requests.
    @queue = []

    # Is execution of the next job on `process.nextTick` already scheduled?
    @scheduled = no

    # The currently running job or `null`.
    @runningJob = null


  # Registers the given func to run for all requests that match the given scope.
  register: (scope, func) ->
    unless typeof scope is 'object'
      throw new TypeError("JobQueue.register(scope, func) scope arg must be an object")
    unless typeof func is 'function'
      throw new TypeError("JobQueue.register(scope, func) func arg must be a function")
    @handlers.push new JobHandler(scope, func)


  # Requests to perform a job with the given attributes.
  add: (request) ->
    if handler = @findHandler(request)
      request.handler = handler
      request.id = handler.computeId(request)
      @queue.push request
      @schedule()


  # ### JobQueue private methods


  # Finds a matching registered handler for the given request.
  findHandler: (request) ->
    for handler in @handlers
      if handler.matches(request)
        return handler
    null


  # Schedules execution of the next job in queue on `process.nextTick`.
  schedule: ->
    return if @scheduled or @runningJob
    @scheduled = yes

    process.nextTick =>
      @scheduled = no
      @executeNextJob()


  # Executes the next job in queue. When done, either schedules execution of the next job or emits
  # ‘drain’.
  executeNextJob: ->
    if request = @queue.shift()
      @executeJob(request)
    else
      @scheduleOrEmitDrain()


  # Executes the given job. When done, either schedules execution of the next job or emits ‘drain’.
  # Assumes that the given request has already been removed from the queue.
  executeJob: (request) ->
    # Mark the job as running (and announce the news)
    @runningJob = request
    @emit 'running', request.handler.id, request

    # Execute the job by running the handler function
    await request.handler.func(request, defer())

    # Mark the job as completed (and announce the news)
    @runningJob = null
    @emit 'complete', request.handler.id, request

    # Fulfil our promise to reschedule or emit ‘drain’
    @scheduleOrEmitDrain()


  # Depending on whether any more jobs are left in queue, either calls `#schedule` to schedule
  # execution on next tick, or emits ‘drain’ event.
  scheduleOrEmitDrain: ->
    if @queue.length > 0
      @schedule()
    else
      @emit 'drain'


# ### JobHandler

# A private helper class that stores a handler registered via `JobQueue#register`, together with its
# scope and options.
class JobHandler
  constructor: (@scope, @func) ->
    @idKeys = (key for own key of @scope).sort()
    @id = ("#{key}:#{value}" for own key, value of @scope).sort().join(':')

  matches: (request) ->
    for own key, value of @scope
      unless request[key] == value
        return no
    yes

  computeId: (request) ->
    (('' + request[key]) for key of @idKeys).join('-')
