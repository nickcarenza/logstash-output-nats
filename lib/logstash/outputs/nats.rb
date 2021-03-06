# encoding: utf-8

require "logstash/outputs/base"
require "logstash/namespace"

require_relative "nats/connection"

# A NATS output for logstash
class LogStash::Outputs::Nats < LogStash::Outputs::Base
  conn = nil

  default :codec, "json"

  config_name "nats"

  # The subject to use
  config :subject, :validate => :string, :required => true

  # The hostname or IP address to reach your NATS instance
  config :host, :validate => :string, :default => "nats://0.0.0.0:4222", :required => true

  # The time to wait before reconnecting to the server when failing to publish
  config :max_reconnect_count, :validate => :number, :default => -1, :required => true

  # The time to wait before reconnecting to the server when failing to publish
  config :reconnect_time_wait, :validate => :number, :default => 1000, :required => true

  # The size of the buffer to use when buffering messages during a reconnect
  config :reconnect_buf_size, :validate => :number, :default => (8 * 1024 * 1024), :required => true

  # The timeout to use when publishing
  config :publish_timeout, :validate => :number, :default => 1000, :required => true

  # This sets the concurrency behavior of this plugin. By default it is :legacy, which was the standard
  # way concurrency worked before Logstash 2.4
  #
  # You should explicitly set it to either :single or :shared as :legacy will be removed in Logstash 6.0
  #
  # When configured as :single a single instance of the Output will be shared among the
  # pipeline worker threads. Access to the `#multi_receive/#multi_receive_encoded/#receive` method will be synchronized
  # i.e. only one thread will be active at a time making threadsafety much simpler.
  #
  # You can set this to :shared if your output is threadsafe. This will maximize
  # concurrency but you will need to make appropriate uses of mutexes in `#multi_receive/#receive`.
  #
  # Only the `#multi_receive/#multi_receive_encoded` methods need to actually be threadsafe, the other methods
  # will only be executed in a single thread
  concurrency :single

  def register
    @codec.on_event &method(:send_to_nats)
  end

  def get_nats_connection
    if @conn == nil
      @conn = NATSConnection.new(
        @host,
        @logger,
        @max_reconnect_count,
        @reconnect_time_wait,
        @reconnect_buf_size,
        @publish_timeout)
    end

    @conn
  end

  def recreate_nats_connection
    if @conn != nil
      @conn.close
    end

    get_nats_connection
  end

  # Needed for logstash < 2.2 compatibility
  # Takes events one at a time
  def receive(event)
    if event == LogStash::SHUTDOWN
      return
    end

    begin
      @codec.encode event
    rescue Exception => e
      @logger.warn("NATS: Error encoding event",
        :event => event,
        :exception => e)
      @logger.debug "NATS: Backtrace: ", :backtrace => e.backtrace
    end
  end

  def send_to_nats(event, payload)
    key = event.sprintf @subject
    @logger.debug "NATS: Publishing event to #{key}"

    begin
      conn = get_nats_connection
      conn.publish key, payload
    rescue Exception => e
      @logger.warn("NATS: failed to send event",
        :event => event,
        :exception => e)
      @logger.debug "NATS: Backtrace: ", :backtrace => e.backtrace
      sleep (@reconnect_time_wait / 1000)
      recreate_nats_connection
      retry
    end
  end
end

