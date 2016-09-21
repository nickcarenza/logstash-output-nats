# encoding: utf-8

require "logstash/outputs/base"
require "logstash/namespace"

require "json"
require "socket"
require "thread"
require "uri"

class NatsConnection
  def initialize(uri)
    super()
    @mutex = Mutex.new
    @uri = uri
  end

  private
  def cleanup
    @socket = nil
    @thread = nil
  end

  private
  def close
    if @thread != nil
      @thread.join
    end

    if @socket != nil
      @socket.close
    end

    cleanup
  end

  private
  def connect
    if @socket == nil
      uri = URI(@uri)
      @socket = TCPSocket.new uri.host, uri.port

      result = receive # this should be an INFO payload
      if !(result =~ /^INFO /)
        # didn't get an info payload, error out
        raise "Unexpected state: #{result}"
      end

      @socket.puts generate_connect_data
      result = receive # this should be an '+OK' payload

      if !(result =~ /^\+OK/)
        raise "Server connection failed: #{result}"
      end

      @thread = Thread.new do
        loop do
          result = receive

          if result =~ /^PING/
            send "PONG"
          end

          sleep 0.1
        end
      end
    end
  end

  public
  def connected?
    # probably should actually ping the server here
    @socket != nil
  end

  private
  def generate_connect_data
    opts = {
      :pendantic => true,
      :verbose => true,
      :ssl_required => false,
      :name => "PureRubyNatsPublisher",
      :lang => "ruby",
      :version => "2.0.0",
    }

    "CONNECT #{opts.to_json}\r\n"
  end

  private
  def generate_publish_data(subject, data)
    if !data.is_a? String
      data = data.to_json
    end

    "PUB #{subject} #{data.length}\r\n#{data}\r\n"
  end

  public
  def publish(subject, data)
    connect
    line = generate_publish_data subject, data
    send line
  end

  private
  def receive
    result = nil

    @mutex.synchronize do
      result = @socket.gets
    end

    result
  end

  private
  def send(data)
    @mutex.synchronize do
      @socket.puts data
    end
  end
end

# A NATS output for logstash
class LogStash::Outputs::Nats < LogStash::Outputs::Base
  config_name "nats"

  default :codec, "json"

  # The subject to use
  config :key, :validate => :string, :required => true

  # The hostname or IP address to reach your NATS instance
  config :host, :validate => :string, :default => "nats://0.0.0.0:4222", :required => true

  # The reconnect retry time
  config :retry_time_wait, :validate => :number, :default => 0.5, :required => true

  # Maximum retry attempts
  config :max_retry_attempts, :validate => :number, :default => 10, :required => true

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

  def close
    puts "close"
  end

  # Needed for logstash < 2.2 compatibility
  # Takes events one at a time
  def receive(event)
    if event == LogStash::SHUTDOWN
      return
    end

    @codec.encode(event)
  end

  def register
    # NATS.on_connect do
    #   @logger.debug("NATS: connected")
    # end

    # NATS.on_error do |e|
    #   @logger.warn("NATS: error", :error => e)
    # end

    # NATS.on_disconnect do |reason|
    #   @logger.warn("NATS: disconnected", :reason => reason)
    # end

    # NATS.on_reconnect do |nats|
    #   @logger.warn("NATS: reconnected", :server => nats.connected_server)
    # end

    # NATS.on_close do
    #   @logger.warn("NATS: connection closed")
    # end

    @codec.on_event(&method(:send_to_nats))
  end

  def send_to_nats(event, payload)
    key = event.sprintf(@key)
    opts = {
      :dont_randomize_servers => !@randomize_servers,
      :reconnect_time_wait => @reconnect_time_wait,
      :max_reconnect_attempts => @max_reconnect_attempts,
      :servers => @servers,
    }

    @logger.debug("NATS: publishing event #{key}")

    begin
      retries ||= 0
      @logger.debug("NATS: connecting to #{opts[:servers]}")
      conn = NatsConnection.new opts[:servers][0]
      conn.publish(key, payload)
    rescue => e
      @logger.warn("NATS: failed to send event",
        :event => event,
        :exception => e,
        :backtrace => e.backtrace)
      sleep @retry_time_wait
      retry if (retries += 1) < @max_retry_attempts
    end
  end
end
