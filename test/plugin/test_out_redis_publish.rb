require 'helpers'

require 'redis'

$channels = []
$messages = []
$subscriber_count = 1

class Redis
  attr_writer :subscriber_count
  
  def initialize(options = {})
  end

  def pipelined
    yield self
  end

  def publish(channel, message)
    if $subscriber_count > 0
      $channels << channel
      $messages << message
    end
    $subscriber_count
  end
end

class RedisPublishOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
    $channels = []
    $messages = []
  end

  CONFIG1 = %[
  ]
  CONFIG2 = %[
    host 192.168.2.3
    port 9999
    db 3
  ]
  CONFIG3 = %[
    path /tmp/foo.sock
  ]

  def create_driver(conf)
    Fluent::Test::BufferedOutputTestDriver.new(Fluent::RedisPublishOutput).configure(conf)
  end

  def test_configure
    # defaults
    d = create_driver(CONFIG1)
    assert_equal "127.0.0.1", d.instance.host
    assert_equal 6379, d.instance.port
    assert_equal nil, d.instance.path
    assert_equal 0, d.instance.db

    # host port db
    d = create_driver(CONFIG2)
    assert_equal "192.168.2.3", d.instance.host
    assert_equal 9999, d.instance.port
    assert_equal nil, d.instance.path
    assert_equal 3, d.instance.db

    # path
    d = create_driver(CONFIG3)
    assert_equal "/tmp/foo.sock", d.instance.path
  end

  def test_write
    d = create_driver(CONFIG1)

    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    d.emit({ "foo" => "bar" }, time)
    d.run

    assert_equal 1, $channels.count
    assert_equal 1, $messages.count
    assert_equal "test", $channels[0]
    assert_equal(%Q[{"foo":"bar","time":#{time}}], $messages[0])
  end

  def test_buffered_write
    d = create_driver(CONFIG1 + %[
      wait true
    ])
    time = Time.parse("2011-01-02 13:14:15 UTC").to_i
    
    $subscriber_count = 0
    d.emit({ "foo" => "bar" }, time)
    assert_raise RuntimeError do
      d.run
    end

    assert_equal 0, $channels.count
    assert_equal 0, $messages.count

    $subscriber_count = 1
    d.emit({ "bar" => "baz" }, time)
    d.run
    
    assert_equal 2, $channels.count
    assert_equal 2, $messages.count
    assert_equal "test", $channels[0]
    assert_equal "test", $channels[1]
    assert_equal(%Q[{"foo":"bar","time":#{time}}], $messages[0])
    assert_equal(%Q[{"bar":"baz","time":#{time}}], $messages[1])
  end
end
