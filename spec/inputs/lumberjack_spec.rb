# encoding: utf-8
require_relative "../spec_helper"
require "stud/temporary"
require 'logstash/inputs/lumberjack'
require "logstash/codecs/plain"
require "logstash/codecs/multiline"
require "logstash/event"
require "lumberjack/client"
require "lumberjack/server"

Thread.abort_on_exception = true
describe LogStash::Inputs::Lumberjack do
  let(:connection) { double("connection") }
  let(:certificate) { LogStashTest.certificate }
  let(:queue)  { [] }
  let(:config)   { { "port" => 0,
                     "ssl_certificate" => certificate.ssl_cert,
                     "ssl_key" => certificate.ssl_key,
                     "type" => "example",
                     "tags" => "lumberjack" } }

  subject(:lumberjack) { LogStash::Inputs::Lumberjack.new(config) }

  context "#register" do
    it "raise no exception" do
      plugin = LogStash::Inputs::Lumberjack.new(config)
      expect { plugin.register }.not_to raise_error
    end
  end

  describe "#processing of events" do
    context "multiline" do
      let(:codec) { LogStash::Codecs::Multiline.new("pattern" => '^2015',
                                                    "what" => "previous",
                                                    "negate" => true) }
      let(:config) { super.merge({ "codec" => codec }) }
      let(:events_map) do
        [
          { "host" => "machineA", "file" => "/var/log/line", "line" => "2015-11-10 10:14:38,907 line 1" },
          { "host" => "machineA", "file" => "/var/log/other", "line" => "2015-11-10 10:14:38,907 xline 1" },
          { "host" => "machineA", "file" => "/var/log/line", "line" => "line 1.1" },
          { "host" => "machineA", "file" => "/var/log/other", "line" => "xline 1.1" },
          { "host" => "machineA", "file" => "/var/log/line", "line" => "2015-11-10 10:16:38,907 line 2" },
          { "host" => "machineA", "file" => "/var/log/other", "line" => "2015-11-10 10:16:38,907 xline 2" },
          { "host" => "machineA", "file" => "/var/log/line", "line" => "line 2.1" },
          { "host" => "machineA", "file" => "/var/log/other", "line" => "xline 2.1" },
          { "host" => "machineA", "file" => "/var/log/line", "line" => "line 2.2" },
          { "host" => "machineA", "file" => "/var/log/other", "line" => "xline 2.2" },
          { "host" => "machineA", "file" => "/var/log/line", "line" => "line 2.3" },
          { "host" => "machineA", "file" => "/var/log/other", "line" => "xline 2.3" },
          { "host" => "machineA", "file" => "/var/log/line", "line" => "2015-11-10 10:18:38,907 line 3" },
          { "host" => "machineA", "file" => "/var/log/other", "line" => "2015-11-10 10:18:38,907 xline 3" }
        ]
      end

      before do
        lumberjack.register
        Thread.new { lumberjack.run(queue) }
      end

      it "should correctly merge multiple events" do
        # This test, cannot currently work without explicitely call a flush
        # the flush is never timebased, if no new data is coming in we wont flush the buffer
        # https://github.com/logstash-plugins/logstash-codec-multiline/issues/11
        events_map.each { |e| lumberjack.create_event(e) { |e| queue << e } }
        lumberjack.stop

        expect(queue.size).to eq(6)
        expect(queue.collect { |e| e["message"] }).to include("2015-11-10 10:14:38,907 line 1\nline 1.1",
                                                              "2015-11-10 10:14:38,907 xline 1\nxline 1.1",
                                                              "2015-11-10 10:16:38,907 line 2\nline 2.1\nline 2.2\nline 2.3",
                                                              "2015-11-10 10:16:38,907 xline 2\nxline 2.1\nxline 2.2\nxline 2.3",
                                                              "2015-11-10 10:18:38,907 line 3",
                                                              "2015-11-10 10:18:38,907 xline 3")
      end
    end
  end

  context "when interrupting the plugin" do
    it_behaves_like "an interruptible input plugin"
  end
end
