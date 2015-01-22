# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"

# Receive events using the lumberjack protocol.
#
# This is mainly to receive events shipped with lumberjack[http://github.com/jordansissel/lumberjack], 
# now represented primarily via the
# https://github.com/elasticsearch/logstash-forwarder[Logstash-forwarder].
#
class LogStash::Inputs::Lumberjack < LogStash::Inputs::Base

  config_name "lumberjack"

  default :codec, "plain"

  # The IP address to listen on.
  config :host, :validate => :string, :default => "0.0.0.0"

  # The port to listen on.
  config :port, :validate => :number, :required => true

  # SSL certificate to use.
  config :ssl_certificate, :validate => :path, :required => true

  # SSL key to use.
  config :ssl_key, :validate => :path, :required => true

  # SSL key passphrase to use.
  config :ssl_key_passphrase, :validate => :password

  # The field name where the client IP address should be stored in each message
  config :client_address_field, :validate => :string, :default => nil

  # TODO(sissel): Add CA to authenticate clients with.

  public
  def register
    require "lumberjack/server"

    @logger.info("Starting lumberjack input listener", :address => "#{@host}:#{@port}")
    @lumberjack = Lumberjack::Server.new(:address => @host, :port => @port,
      :ssl_certificate => @ssl_certificate, :ssl_key => @ssl_key,
      :ssl_key_passphrase => @ssl_key_passphrase)
  end # def register

  public
  def run(output_queue)
    @lumberjack.run do |l, client_address|
      @codec.decode(l.delete("line")) do |event|
        decorate(event)
        if !@client_address_field.nil?
          event[@client_address_field] = client_address
        end
        l.each { |k,v| event[k] = v; v.force_encoding(Encoding::UTF_8) }
        output_queue << event
      end
    end
  end # def run
end # class LogStash::Inputs::Lumberjack
