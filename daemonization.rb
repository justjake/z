#!/usr/bin/env ruby
#
# A single file library for fast-executing and long-lived ruby (command line)
# tools. Integrate with your main executable to receive a daemonizing and thus
# fast-loading command server.
#
# This file can also be used as a client to connect to an existing server.
#
# This tools aims to have as few dependencies as possible, making it much
# faster to load, compared to an average Ruby program.

module Z
  # Raised if this module encounters any internal errors.
  class Error < ::StandardError; end

  # Sent as the first message in our protocol to execute a message.
  EXECUTE = '/v0/execute'.freeze

  # Channel performs blocking communications of fixed size using an underlying
  # IO object. This makes talking over a socket safer, since we guarantee that
  # entire messages are received before moving on, without relying on finicky
  # delimiters.
  class Channel
    def self.pipe
      r, w = ::IO.pipe
      [new(r), new(w)]
    end

    def initialize(io)
      @io = io
    end

    def to_io
      @io
    end

    def <<(message)
      if message.nil?
        raise ArgumentError.new("Message cannot be nil")
      end

      if message.bytesize.size > 4
        raise ArgumentErrorn.new("Message length cannot be stored in a u32")
      end

      encoded = [message.bytesize, message].pack("NA*")
      @io.write(encoded)
      @io.flush
    end

    def receive
      size_raw = @io.read(4)
      return nil if size_raw.nil?

      size = size_raw.unpack('N').first
      if size.nil?
        raise Error.new("Not enough bytes for u32: #{size_raw.inspect}")
      end
      message = @io.read(size)
      if message.bytesize < size
        raise Error.new("Message underflow: expected #{size} bytes, got #{message.bytesize}")
      end
      message
    end
  end

  # Client connects to a server socket and requests the execution of a command.
  # The client then blocks until the exit status is received, which it returns.
  class Client
    # Short-hand for performing a standard client execute request to the UNIX
    # socket at the given path. Create a new client, and send this process's
    # state across it.
    #
    # @param socket_path [String]
    #
    # @example usage in a simple client script
    #
    #   socket_path = File.expand_path("~/.my-app-socket")
    #   exit_code = Z::Client.execute(socket_path
    #   exit(exit_code)
    #
    def self.execute(socket_path)
      client = new(socket_path)
      client.execute(
        ::Dir.pwd,
        [$0, *::ARGV],
        ::STDIN,
        ::STDOUT,
        ::STDERR
      )
    end

    def initialize(socket_path)
      @socket_path = socket_path
    end

    def execute(cwd, argv, stdin, stdout, stderr)
      channel << EXECUTE
      channel << cwd
      channel << argv.join("\0")
      channel.to_io.send_io(stdin)
      channel << "sent stdin"
      channel.to_io.send_io(stdout)
      channel << "sent stdout"
      channel.to_io.send_io(stderr)
      channel << "sent stderr"

      exit_code = channel.receive
      Integer(exit_code)
    end

    private

    def channel
      @channel ||= Channel.new(socket)
    end

    def socket
      @socket ||= connect
    end

    def connect
      require 'socket'
      ::UNIXSocket.new(@socket_path)
      # TODO: error handling? Retries?
    end
  end

  # Handles a single client connection on the server side.
  class ServerHandler
    def initialize(channel)
      @channel = channel
    end

    def to_io
      channel.to_io
    end

    def receive
      request = channel.receive
      unless request == EXECUTE
        raise Error.new("Unsupported request: #{requrest.inspect}")
      end
      receive_execute
    end

    def send_exit_code(exit_code)
      raise ArgumentError.new("Invalid exit code: #{exit_code}") unless exit_code < 255
      channel << exit_code.to_s
    end

    private

    attr_reader :channel

    def read_execute
      cwd = channel.receive
      argv = channel.receive.split("\0")
      stdin = channel.to_io.recv_io
      channel.receive
      stdout = channel.to_io.recv_io
      channel.receive
      stderr = channel.to_io.recv_io
      channel.receive

      # return all data
      {
        request: EXECUTE,
        cwd: cwd,
        argv: argv,
        stdin: stdin,
        stdout: stdout,
        stderr: stderr,
      }
    end
  end

  # Server accepts incoming connections on a UNIX socket, returning
  # ServerHandler instances.
  class Server
    def initialize(socket_path)
      @socket_path = socket_path
    end

    def to_io
      socket
    end

    def accept
      ServerHandler.new(socket.accept)
    end

    private

    def socket
      require 'socket'
      @socket ||= ::UNIXServer.new(@socket_path)
    end
  end

  # Example command-line program that demonstrates both client and server
  class CLI
    def option_parser
      require 'optparse'
      name = $0
      @option_parser ||= ::OptionParser.new do |o|
        o.banner = "#{name} - ruby daemonization tool"
        o.version = "0.0.0"
        o.separator <<-EOS
Usage:
  #{name} server <socket>
    Start the example server listening to the given socket.
    The example server accepts connections, and just prints out inspection data
    to them for debugging.

  #{name} client <socket> [ <args> ]
    Connect to a server and attempt to execute the command.
EOS
      end
    end

    def run(argv)
      args = option_parser.parse(argv)

      unless %w(client server).include?(args.first)
      end

      unless args.size > 2
        $stderr.puts("Must specify a command and a <socket> path")
        $stderr.puts(option_parser)
        exit 130
      end

      case args.first
      when "client"
        exit(::Z::Client.execute(args[1]))
      when "server"
        exit(serve(args[1]))
      else
        $stderr.puts("Unknown command #{args.first.inspect}")
        $stderr.puts(option_parser)
        exit 130 # cannot execute
      end
    end

    def logger
      require 'logger'
      @logger ||= ::Logger.new($stderr)
    end

    def serve(socket_path)
      # Since we run each handler in a thread...
      Thread.abort_on_exception = true

      server = ::Z::Server.new(socket_path)
      loop do
        handler = server.accept
        logger.info("Accepted new connection: #{handler}")
        Thread.new { handle(handler) }
      end
    end

    def handle(handler)
      context = handler.receive
      logger.info("Received a #{context[:request]} request.")

      # Do some shenanigans or something
      context[:stdout].puts context_info.inspect
      context[:stderr].puts "Type any text, then press return: "
      some_string = context[:stdin].gets
      context[:stdout].puts some_string.strip
      context[:stdout].puts "all done!"

      logger.info("Completed handling #{handler}. Returning exit code.")

      # just for fun, a error exit code...
      handler.send_exit_code(76)
    end
  end
end

# If this file was executed as a script, run our CLI.
if __FILE__ == $0
  ::Z::CLI.new.run
end
