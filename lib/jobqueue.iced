# Job queue runs a set of jobs serially, providing additional request merging and introspection
# capabilities.

# Imports
{ EventEmitter } = require 'events'

# ### JobQueue public API

# JobQueue is the class returned when you `require('jobqueue')`.
module.exports =
class JobQueue extends EventEmitter
  constructor: ->
    @handlers = []

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
      request.id = handler.computeId(request)
      await process.nextTick defer()

      @emit 'running', handler.id, request
      await handler.func(request, defer())
      @emit 'complete', handler.id, request
      @emit 'drain'

  # ### JobQueue private methods

  # Finds a matching registered handler for the given request.
  findHandler: (request) ->
    for handler in @handlers
      if handler.matches(request)
        return handler
    null


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
