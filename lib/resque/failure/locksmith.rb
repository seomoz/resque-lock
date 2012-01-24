module Resque
  module Failure

    # If you use the Resque::Plugins::Lock plugin you're going to want to add
    # Locksmith to your failure backend chain. The reason for this is that if
    # your job blows up or dies do to a dirty exit; the lock will not be removed.
    #
    # What this results in is a job that silently stops running and leads to
    # a fit of troubleshooting and anger.
    #
    # You'll likely want to use the Multiple failure backends wrapper:
    # Resque::Failure::Multiple.classes = [Resque::Failure::Redis, Resque::Failure::Locksmith]
    # Resque::Failure.backend = Resque::Failure::Multiple
    class Locksmith < Base

      def save
        work = payload['class']
        if work.respond_to?(:lock)
          lock = work.lock(*payload['args'])
          Resque.redis.del(lock)
        end

      end

    end
  end
end
