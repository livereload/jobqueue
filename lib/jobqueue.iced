# Job queue runs a set of jobs serially, providing additional request merging and introspection
# capabilities.

# Imports
{ EventEmitter } = require 'events'
{ inspect }      = require 'util'
debug            = require('debug')('jobqueue')

# ### JobQueue public API

# JobQueue is the class returned when you `require('jobqueue')`.
module.exports =
class JobQueue extends EventEmitter
  constructor: ->
    # Handlers registered via `#register`.
    @handlers = []

    # All queued jobs indexed by job id.
    @idsToJobs = {}
    # All queued jobs in the order of enqueueing.
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

    handler = new JobHandler(scope, func)
    @handlers.push handler
    debug "Registered handler #{handler.id} with ID keys #{JSON.stringify(handler.idKeys)}"


  # Requests to perform a job with the given attributes.
  add: (request) ->
    if handler = @findHandler(request)
      request.handler = handler
      request.id = handler.computeId(request)
      debug "Adding job #{stringifyRequest request} with request id #{request.id}, handled by #{handler.id}"

      if prior = @idsToJobs[request.id]
        debug "Found existing job for request id '#{request.id}': #{stringifyRequest prior}"
        if prior.handler == handler
          handler.merge(request, prior)
          @removeRequestFromQueues prior
          debug "Merged the old job into the new one: #{stringifyRequest request}"
        else
          throw new Error("Attempted to add a request that matches another request with the same id, but different handler: id is '#{request.id}', request is #{stringifyRequest request}")

      @addRequestToQueue request
      @schedule()
    else
      throw new Error("No handlers match the added request: " + stringifyRequest(request))


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
    if request = @extractNextQueuedRequest()
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


  # Add the given request to the underlying data structure.
  addRequestToQueue: (request) ->
    @queue.push request
    @idsToJobs[request.id] = request


  # Remove the given request to the underlying data structure.
  removeRequestFromQueues: (request) ->
    if (index = @queue.indexOf request) >= 0
      @queue.splice index, 1
    delete @idsToJobs[request.id]


  # Extract (i.e. remove and return) the next request from the underlying data structure.
  extractNextQueuedRequest: ->
    if request = @queue.shift()
      delete @idsToJobs[request.id]
      request
    else
      null


# ### JobHandler

# A private helper class that stores a handler registered via `JobQueue#register`, together with its
# scope and options.
class JobHandler
  constructor: (@scope, @func) ->
    @idKeys = (key for own key of @scope).sort()
    @id = ("#{key}:#{value}" for own key, value of @scope).sort().join('-')

  matches: (request) ->
    for own key, value of @scope
      unless request[key] == value
        return no
    yes

  computeId: (request) ->
    ("#{key}:#{request[key]}" for key in @idKeys).join('-')

  merge: (request, prior) ->
    for own key, oldValue of prior
      newValue = request[key]
      if newValue != oldValue
        if (Array.isArray newValue) and (Array.isArray oldValue)
          newValue.splice 0, 0, oldValue...
          continue
        throw new Error "No default strategy for merging key '#{key}' of old request into new request; request id is #{request.id}, old request is #{stringifyRequest prior}, new request is #{stringifyRequest request}"


# ### Helper functions (public API for debugging and testing purposes only)

# Returns a string representation of the given request for debugging and logging purposes.
JobQueue.stringifyRequest = stringifyRequest = (request) ->
  ("#{key}:#{stringifyValue value}" for own key, value of request when !(key in ['id', 'handler'])).join('-')

# Returns a string representation of the given value. (We don't want to use JSON.stringify because
# it might throw, and this method should be useful for debugging errors that involve garbage
# arguments.)
JobQueue.stringifyValue = stringifyValue = (value) ->
  if typeof value is 'string'
    value
  else
    inspect(value)
