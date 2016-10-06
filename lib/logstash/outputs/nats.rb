# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"
require "nats/client"
require "eventmachine"

# An example output that does nothing.
class LogStash::Outputs::Nats < LogStash::Outputs::Base
  config_name "nats"

  default :codec, "json"

  # The hostname(s) of your Nats server(s)
  #
  # For example:
  # [source,ruby]
  #     ["nats://127.0.0.1:4222", "nats://127.0.0.2:4222"]
  config :host, :validate => :array, :default => ["nats://127.0.0.1:4222"]

  # Don't shuffle the host list for nats connection.
  config :dont_randomize_servers, :validate => :boolean, :default => false

  # Interval for reconnecting to failed Redis connections
  config :reconnect_time_wait, :validate => :number, :default => 1

  # The name of a Nats subject. Dynamic names are
  # valid here, for example `logstash-%{type}`.
  config :subject, :validate => :string, :required => false

  public
  def register
    @host.shuffle!
    # connect {
    #   @logger.debug("NATS connected")
    # }
    connect {
      @logger.info("nats: NATS connected")
    }
    @codec.on_event(&method(:send_to_nats))
    @logger.info("nats: Output registered")
  end # def register

  public
  def receive(event)
    @logger.info("nats: Received event")
    # TODO(sissel): We really should not drop an event, but historically
    # we have dropped events that fail to be converted to json.
    # TODO(sissel): Find a way to continue passing events through even
    # if they fail to convert properly.
    begin
      @logger.info("nats: Encoding event")
      @codec.encode(event)
    rescue LocalJumpError
      # This LocalJumpError rescue clause is required to test for regressions
      # for https://github.com/logstash-plugins/logstash-output-redis/issues/26
      # see specs. Without it the LocalJumpError is rescued by the StandardError
      raise
    rescue StandardError => e
      @logger.warn("nats: Error encoding event", :exception => e,
        :event => event)
    end
  end # def event

  private
  def connect(&blk)
    params = {
      :servers => @host,
      :reconnect_time_wait => @reconnect_time_wait,
      :dont_randomize_servers => @shuffle_hosts,
    }

    @logger.info(params)

    # NATS.on_connect do
    #   @logger.debug("Nats connected")
    # end

    # NATS.on_connect {
    #   @logger.debug("Nats connected")
    # }

    NATS.on_error do |e|
      @logger.warn("nats: NATS error", :error => e)
    end

    NATS.on_disconnect do |reason|
      @logger.warn("nats: NATS disconnected", :reason => reason)
    end

    NATS.on_reconnect do |nats|
      @logger.warn("nats: NATS reconnected", :server => nats.connected_server)
    end

    NATS.on_close do
      @logger.warn("nats: NATS connection closed")
    end

    Thread.new { NATS.start(params, &blk) }
  end # def connect

  def send_to_nats(event, payload)
    # How can I do this sort of thing with codecs?
    subject = event.sprintf(@subject)

    @logger.info("nats: Publishing event", :subject => subject)

    begin
      NATS.publish(subject, payload)
    rescue => e
      @logger.warn("nats: Failed to send event to Nats",
        :event => event,
        :exception => e,
        :backtrace => e.backtrace)
      sleep @reconnect_time_wait
      retry
    end
  end
end # class LogStash::Outputs::Example