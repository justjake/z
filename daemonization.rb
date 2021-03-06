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
        raise ::ArgumentError.new("Message cannot be nil")
      end

      unless fit_u32(message.bytesize.size)
        raise ::ArgumentError.new("Message length cannot be stored in a u32: #{message.inspect}")
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

    private

    def fit_u32(before)
      after = [before].pack("N").unpack("N").first
      before == after
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
  #
  # TODO: add a yielding mathod that yields a context, and then closes the
  # context if no excpetions are raised?
  #
  # TODO: what if the client mysteriously dies...?
  class ServerHandler
    def initialize(channel)
      @channel = channel
      @got_ios = []
    end

    def to_io
      channel.to_io
    end

    def receive
      request = channel.receive
      unless request == EXECUTE
        raise Error.new("Unsupported request: #{request.inspect}")
      end
      receive_execute
    end

    def send_exit_code(exit_code)
      raise ArgumentError.new("Invalid exit code: #{exit_code}") unless exit_code < 255
      channel << exit_code.to_s
    end

    def close_with_exit_code(exit_code)
      send_exit_code(exit_code)
    ensure
      @got_ios.each(&:close)
      @channel.to_io.close
    end

    private

    attr_reader :channel

    def receive_execute
      cwd = channel.receive
      argv = channel.receive.split("\0")
      stdin = channel.to_io.recv_io
      channel.receive
      stdout = channel.to_io.recv_io
      channel.receive
      stderr = channel.to_io.recv_io
      channel.receive

      @got_ios.concat([stdin, stdout, stderr]).uniq!

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
  #
  # TODO: if a socket at that path exists, see if it's alive. If not, remove it and start serving
  #       if it is alive, raise an error.
  class Server
    def initialize(socket_path)
      @socket_path = socket_path
    end

    def to_io
      socket
    end

    def accept
      client = socket.accept
      ServerHandler.new(Channel.new(client))
    end

    def close
      socket.close
      File.delete(@socket_path) if File.exist?(@socket_path)
    end

    private

    def socket
      require 'socket'
      @socket ||= ::UNIXServer.new(@socket_path)
    end
  end

  # Here's where our complexity expands significanlty. The Framework allows you
  # to provide just a few methods to create a fully auto-daemonized application.
  # You can think of Framework like Zeus for your command-line apps.
  #
  # When you call #run, we try to connect to a running, daemonized version of your app.
  # If no daemon is found, a new one is forked into the background, and then we connect.
  class Framework
    def initialize(app_name)
      @app_name = app_name
      @load_app_proc = nil
      @run_app_proc = nil
    end

    def to_load_app(&block)
      @load_app_proc = block
      self
    end

    def to_run_app(&block)
      @run_app_proc = block
      self
    end

    def run
      raise Error.new("You must call #to_load_app { ... } first") unless @load_app_proc
      raise Error.new("You must call #to_run_app { ... } first") unless @run_run_app

      # best case: the daemon is already running
      client = new_client
      if client
        exit client.execute(
          ::Dir.pwd,
          [$0, *::ARGV],
          ::STDIN,
          ::STDOUT,
          ::STDERR
        )
      end

      # if we're still here... we need to create a server
      File.mkdir(dir) unless File.exist?(dir)
      logger.info("No daemon running")
      server_is_ready, write_when_ready = ::Z::Channel.pipe
      server_pid = fork do
        Process.daemon
        daemon_main(write_when_ready)
      end
      $stderr.puts("Started new daemon PID: #{pid}, see #{log_path} for more info.")
      # block until the server is ready
      server_is_ready.receive
    end

    def daemon_main(on_ready)
      logger.info("Daemonized new process")

      load_start = Time.new
      @to_load_app.call
      load_end = Time.new
      logger.info("Loaded in #{load_end - load_start} seconds.")

      logger.info("Starting server listening on #{socket_path}...")
      server = ::Z::Server.new(socket_path)
      logger.info("Ready!")
      on_ready << "ready!"

      loop do
        handler = server.accept
        log.info("Handling new connection #{handler}")
        fork { handle(handler) }
      end
    ensure
      server.close
    end

    def handle(handler)
      original_context = {
        cwd: ::Dir.pwd,
        argv: [$0, *::ARGV],
        stdin: $stdin.dup,
        stdout: $stdout.dup,
        stderr: $stderr.dup,
      }

      begin
        context = handler.receive
        $stdin.reopen(context[:stdin])
        $stdout.reopen(context[:stdout])
        $stderr.reopen(context[:stderr])
        ::ARGV.empty
        ::ARGV.concat(context[:argv][1..-1])
        $0 = context[:argv].first
        Dir.chdir(context[:cwd])
      rescue => err
        logger.error("Internal error: #{err.class} #{err}")
        logger.error(err.backtrace.join("\n"))
        handler.close_with_exit_code(130) # TODO: right exit code?
        raise
      end

      begin
        exit_code = @run_app_proc.call(original_context)
        handler.close_with_exit_code(exit_code)
        handler = nil
      rescue => err
        logger.error("Application error: #{err.class} #{err}")
        logger.error(err.backtrace.join("\n"))
        handler.close_with_exit_code(1)
        handler = nil
        raise
      rescue ::SystemExit => err
        handler.close_with_exit_code(err.status)
        handler = nil
      ensure
        handler.close_with_exit_code(255) if handler
      end
    end

    def new_client
      ::Z::Client.new(socket_path)
    rescue Errno::ENOENT, Errno::ECONNREFUSED
      nil
    end

    def dir
      @dir ||= ::File.expand_path(File.join('~', @app_name))
    end

    def socket_path
      @socket_path ||= File.join(dir, 'control.sock')
    end

    def log_path
      @log_path ||= File.join(dir, 'log')
    end

    def logger
      require 'logger'
      @logger ||= ::Logger.new(log_path)
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

      unless args.size >= 2
        $stderr.puts("Must specify a command and a <socket> path")
        $stderr.puts(option_parser)
        exit 130
      end

      case args.first
      when "client"
        exit(::Z::Client.execute(args[1]))
      when "server"
        exit(serve(args[1]))
      when "pry"
        require 'pry'
        binding.pry
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
      logger.info("Started server at path #{socket_path}")
      loop do
        handler = server.accept
        logger.info("Accepted new connection: #{handler}")
        Thread.new { handle(handler) }
      end
    ensure
      server.close
    end

    def handle(handler)
      context = handler.receive
      logger.info("Received a #{context[:request]} request.")

      # Do some shenanigans or something
      context[:stderr].puts "hello from the server!"
      context[:stdout].puts context.inspect
      context[:stderr].puts "Type any text, then press return: "
      some_string = context[:stdin].gets
      context[:stdout].puts "You wrote: #{some_string.strip.inspect}"
      context[:stdout].puts "all done!"

      logger.info("Completed handling #{handler}. Returning exit code.")

      # just for fun, a error exit code...
      handler.close_with_exit_code(76)
    rescue => err
      logger.error("Error in request: #{err.class} #{err}")
      logger.error(err.backtrace.join("\n"))
      handler.send_exit_code(254)
    end
  end
end

# If this file was executed as a script, run our CLI.
if __FILE__ == $0
  ::Z::CLI.new.run(ARGV)
end
