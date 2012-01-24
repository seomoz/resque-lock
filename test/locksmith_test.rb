require 'test/unit'
require 'resque'
require 'resque/plugins/lock'
require 'resque/failure/locksmith'


class LocksmithTest < Test::Unit::TestCase

  class Job
    extend Resque::Plugins::Lock
  end

  def test_should_delete_lock
    lock = Job.lock('whatever')
    Job.before_enqueue_lock('whatever')

    payload = {'class' => Job, 'args' => 'whatever'}
    smith = Resque::Failure::Locksmith.new(nil, nil, nil, payload)
    smith.save

    assert_nil Resque.redis.get(lock)
  end

end
