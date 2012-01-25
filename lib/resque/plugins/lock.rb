module Resque
  module Plugins
    # If you want only one instance of your job queued at a time,
    # extend it with this module.
    #
    # For example:
    #
    # require 'resque/plugins/lock'
    #
    # class UpdateNetworkGraph
    #   extend Resque::Plugins::Lock
    #
    #   def self.perform(repo_id)
    #     heavy_lifting
    #   end
    # end
    #
    # No other UpdateNetworkGraph jobs will be placed on the queue,
    # the QueueLock class will check Redis to see if any others are
    # queued with the same arguments before queueing. If another
    # is queued the enqueue will be aborted.
    #
    # If you want to define the key yourself you can override the
    # `lock` class method in your subclass, e.g.
    #
    # class UpdateNetworkGraph
    #   extend Resque::Plugins::Lock
    #
    #   # Run only one at a time, regardless of repo_id.
    #   def self.lock(repo_id)
    #     "network-graph"
    #   end
    #
    #   def self.perform(repo_id)
    #     heavy_lifting
    #   end
    # end
    #
    # The above modification will ensure only one job of class
    # UpdateNetworkGraph is running at a time, regardless of the
    # repo_id. Normally a job is locked using a combination of its
    # class name and arguments.
    module Lock
      THREE_DAYS = 3 * 24 * 60 * 60
      THIRTY_MINUTES = 30 * 60 * 60

      def queue_timeout
        THREE_DAYS
      end

      def perform_timeout
        THIRTY_MINUTES
      end

      # Override in your job to control the lock key. It is
      # passed the same arguments as `perform`, that is, your job's
      # payload.
      def lock(*args)
        "lock:#{name}-#{args.join('-')}"
      end

      def before_enqueue_lock(*args)
        redis = Resque.redis
        key = lock(*args)

        old_ttl, acquired, _ = redis.multi do
          redis.ttl key
          redis.setnx key, true
          redis.expire key, queue_timeout
        end

        if acquired == 1
          true
        else
          # reset the ttl to what it was since we failed to acquire it
          redis.expire key, old_ttl
          false
        end
      end

      def around_perform_lock(*args)
        key = lock(*args)
        begin
          Resque.redis.expire key, perform_timeout
          yield
        ensure
          # Always clear the lock when we're done, even if there is an
          # error.
          Resque.redis.del(key)
        end
      end

      def on_failure_lock(error, *args)
        Resque.redis.del(lock(*args))
      end

    end
  end
end

