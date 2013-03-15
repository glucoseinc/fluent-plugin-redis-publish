module Fluent
  class RedisPublishOutput < Fluent::BufferedOutput
    Fluent::Plugin.register_output('redis_publish', self)

    config_param :host,   :string,  :default => '127.0.0.1'
    config_param :path,   :string,  :default => nil
    config_param :port,   :integer, :default => 6379
    config_param :db,     :integer, :default => 0
    config_param :format, :string,  :default => 'json'
    config_param :wait,   :bool,    :default => false
    config_param :include_time, :bool,  :default => true

    attr_reader :redis

    def initialize
      super
      require 'redis'
      require 'json'
      require 'msgpack'
    end

    def configure(conf)
      super
    end

    def start
      super

      if @path
        @redis = Redis.new(:path => @path, :db => @db)
      else
        @redis = Redis.new(:host => @host, :port => @port, :db => @db);
      end
    end

    def shutdown
      super
    end

    def format(tag, time, record)
      [tag, time, record].to_msgpack
    end

    def write(chunk)
      if @wait
        write_buffered(chunk)
      else
        write_pipelined(chunk)
      end
    end

    private
    def serialize_record(tag, time, record)
      if @include_time
        record["time"] = time
      end

      if @format == "json"
        record.to_json
      else
        record.to_msgpack
      end
    end
      
    def write_pipelined(chunk)
      @redis.pipelined do
        chunk.msgpack_each do |(tag, time, record)|
          @redis.publish(tag, serialize_record(tag, time, record))
        end
      end
    end

    def write_buffered(chunk)
      first_record = true
      chunk.msgpack_each do |(tag, time, record)|
        subscribers = @redis.publish(tag, serialize_record(tag, time, record))
        # fluent's delivery policy is 'At most once':
        # don't raise if we've already published any message.
        if first_record and subscribers == 0
          raise 'no subscriber is listening'
        end
        first_record = false
      end
    end
  end
end
