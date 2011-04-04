require File.expand_path('../../lib/stalker', __FILE__)
require 'contest'
require 'mocha'

module Stalker
  def log(msg); end
  def log_error(msg); end
end

class StalkerBeanstalkStyleTest < Test::Unit::TestCase
  setup do
    results = `pgrep beanstalkd`
    raise "Can not run tests, beanstalkd not running" if $?.exitstatus != 0
    
    Stalker.clear!
    $result = -1
    $handled = false
  end

  def with_an_error_handler(opts={})
    Stalker.error do |e, job_name, args, job, style_opts|
      $handled = e.class
      $job_name = job_name
      $job_args = args
      $job = job
      $style_opts = style_opts
      job.delete unless opts[:no_delete]
    end
  end

  # First tests are the same as stalker_test but enable beanstalk_style. Should act the same as standard
  test "enqueue and work a job" do
    val = rand(999999)
    Stalker.job('my.job') { |args, job, opts| $result = args['val'] }
    Stalker.enqueue('my.job', {:val => val}, {}, true, {})
    Stalker.prep
    Stalker.work_one_job
    assert_equal val, $result
  end
  
  test "invoke error handler when defined" do
    with_an_error_handler
    Stalker.job('my.job') { |args, job, opts| fail }
    Stalker.enqueue('my.job', {:foo => 123}, {}, true, {})
    Stalker.prep
    Stalker.work_one_job
    assert $handled
    assert_equal 'my.job', $job_name
    assert_equal({'foo' => 123}, $job_args)
  end
  
  test "should be compatible with legacy error handlers" do
    exception = StandardError.new("Oh my, the job has failed!")
    Stalker.error { |e| $handled = e }
    Stalker.job('my.job') { |args, job, opts| raise exception }
    Stalker.enqueue('my.job', {:foo => 123}, {}, true, {})
    Stalker.prep
    Stalker.work_one_job
    assert_equal exception, $handled
  end
  
  test "continue working when error handler not defined" do
    Stalker.job('my.job') { fail }
    Stalker.enqueue('my.job', {}, {}, true, {})
    Stalker.prep
    Stalker.work_one_job
    assert_equal false, $handled
  end
  
  test "exception raised one second before beanstalk ttr reached" do
    with_an_error_handler
    Stalker.job('my.job') { sleep(3); $handled = "didn't time out" }
    Stalker.enqueue('my.job', {}, {:ttr => 2}, true, {})
    Stalker.prep
    Stalker.work_one_job
    assert_equal Stalker::JobTimeout, $handled
  end
  
  test "before filter gets run first" do
    Stalker.before { |name| $flag = "i_was_here" }
    Stalker.job('my.job') { |args, job, opts| $handled = ($flag == 'i_was_here') }
    Stalker.enqueue('my.job', {}, {}, true, {})
    Stalker.prep
    Stalker.work_one_job
    assert_equal true, $handled
  end
  
  test "before filter passes the name of the job" do
    Stalker.before { |name| $jobname = name }
    Stalker.job('my.job') { true }
    Stalker.enqueue('my.job', {}, {}, true, {})
    Stalker.prep
    Stalker.work_one_job
    assert_equal 'my.job', $jobname
  end
  
  test "before filter can pass an instance var" do
    Stalker.before { |name| @foo = "hello" }
    Stalker.job('my.job') { |args, job, opts| $handled = (@foo == "hello") }
    Stalker.enqueue('my.job', {}, {}, true, {})
    Stalker.prep
    Stalker.work_one_job
    assert_equal true, $handled
  end
  
  test "before filter invokes error handler when defined" do
    with_an_error_handler
    Stalker.before { |name| fail }
    Stalker.job('my.job') {  }
    Stalker.enqueue('my.job', {:foo => 123}, {}, true, {})
    Stalker.prep
    Stalker.work_one_job
    assert $handled
    assert_equal 'my.job', $job_name
    assert_equal({'foo' => 123}, $job_args)
  end
  
  # The following test beanstalk_style functionality
  test "job gets its Beanstalker::Job instance when beanstalk_style is set" do
    val = rand(999999)
    Stalker.job('mybeanstalk.job') { |args, job, opts| $result = job.class }
    Stalker.enqueue('mybeanstalk.job', {:val => val}, {}, true)
    Stalker.prep
    Stalker.work_one_job
    assert_equal Beanstalk::Job, $result
  end
  
  test "beanstalk_style job gets style_opts" do
    style_opts = {'test' => 42}
    
    Stalker.job('style_opts.job') { |args, job, opts| $result = opts['test'] }
    Stalker.enqueue('style_opts.job', {'an_arg' => "help"}, {:ttr => 100}, true, style_opts)
    Stalker.prep
    Stalker.work_one_job
    assert_equal style_opts['test'], $result
  end
  
  test "beanstalk_style job can delete itself when enqueued with explicit_delete" do
    val = rand(999999)
    Stalker.job('self_delete.job') { |args, job, opts| $result = args['val']; job.delete }
    Stalker.enqueue('self_delete.job', {:val => val}, {}, true, {:explicit_delete => true})
    Stalker.prep
    Stalker.work_one_job
    assert_equal val, $result
    assert_equal 0, Stalker.beanstalk.stats_tube('self_delete.job')['current-waiting']
  end
  
  test "run_job_outside_of_stalker_timeout" do
    Stalker.job('my.job') do |args, job, opts| 
      job.touch
      (0..2).each { |c| sleep 1; job.touch }
      $handled = true
     end
    Stalker.enqueue('my.job', {}, {:ttr => 2}, true, {:run_job_outside_of_stalker_timeout => true})
    Stalker.prep
    Stalker.work_one_job
    assert_equal true, $handled
  end
 
end
