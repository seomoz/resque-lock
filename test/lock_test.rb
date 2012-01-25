require 'ruby-debug'
require 'test/unit'
require 'resque'
require 'resque/plugins/lock'

$counter = 0

class LockTest < Test::Unit::TestCase
  class Job
    extend Resque::Plugins::Lock
    @queue = :lock_test

    def self.queue_timeout
      2
    end

    def self.perform
      raise "Woah woah woah, that wasn't supposed to happen"
    end
  end

  def setup
    Resque.redis.del('queue:lock_test')
    Resque.redis.del(Job.lock)
  end

  def test_lint
    assert_nothing_raised do
      Resque::Plugin.lint(Resque::Plugins::Lock)
    end
  end

  def test_version
    major, minor, patch = Resque::Version.split('.')
    assert_equal 1, major.to_i
    assert minor.to_i >= 17
    assert Resque::Plugin.respond_to?(:before_enqueue_hooks)
  end

  def test_lock
    3.times { Resque.enqueue(Job) }

    assert_equal 1, Resque.redis.llen('queue:lock_test')
  end

  def test_ttl
    Resque.enqueue(Job)
    assert_equal 'true', Resque.redis.get(Job.lock)
    assert_equal 'true', Resque.redis.get(Job.lock)

    sleep (Job.queue_timeout + 1)

    assert_nil Resque.redis.get(Job.lock)
  end

  def test_doesnt_update_ttl_when_not_acquiring_lock
    Resque.enqueue(Job)
    sleep 1
    Resque.enqueue(Job)

    assert_equal (Job.queue_timeout - 1), Resque.redis.ttl(Job.lock)
  end

  def test_failure_hook_removes_lock
    Job.before_enqueue_lock
    assert Resque.redis.exists(Job.lock)
    Job.on_failure_lock(RuntimeError.new)
    assert !Resque.redis.exists(Job.lock)
  end
end
