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

    def self.perform_timeout
      1
    end

    def self.during_perform(&block)
      @perform_hook = block
    end

    def self.perform
      @perform_hook.call
    end
  end

  def setup
    Resque.redis.del('queue:lock_test')
    Resque.redis.del(Job.lock)
    Job.during_perform { }
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
    assert Resque::Plugin.respond_to?(:before_dequeue_hooks)
    assert Resque::Plugin.respond_to?(:failure_hooks)
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

  def test_resets_ttl_during_perform
    lock_ttl = nil
    Job.during_perform do
      lock_ttl = Resque.redis.ttl(Job.lock)
    end

    Resque.enqueue(Job)
    Resque.reserve(:lock_test).perform

    assert_equal Job.perform_timeout, lock_ttl
  end

  def test_failure_hook_removes_lock
    Resque.enqueue(Job)
    assert Resque.redis.exists(Job.lock)
    Resque.reserve(:lock_test).fail(StandardError.new)
    assert !Resque.redis.exists(Job.lock)
  end

  def test_dequeue_hook_removes_lock
    Resque.enqueue(Job)
    Resque.dequeue(Job)
    assert_nil Resque.redis.get(Job.lock)
  end
end
