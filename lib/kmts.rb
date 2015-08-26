require 'uri'
require 'socket'
require 'net/http'
require 'fileutils'
require 'kmts/saas'

class KMError < StandardError; end

class KMTS
  @km_inst   = {}
  @logs      = {}
  @host      = 'trk.kissmetrics.com:80'
  @log_dir   = '/tmp'
  @to_stderr = true
  @use_cron  = false
  @dryrun    = false

  class InitError < StandardError; end

  class << self
    def init(options={})
      default = {
        :host      => @host,
        :log_dir   => @log_dir,
        :to_stderr => @to_stderr,
        :use_cron  => @use_cron,
        :dryrun    => @dryrun
      }
      options = default.merge(options)

      begin
        @host      = options[:host]
        @log_dir   = options[:log_dir]
        @use_cron  = options[:use_cron]
        @to_stderr = options[:to_stderr]
        @dryrun    = options[:dryrun]
      rescue Exception => e
        log_error(e, 'CLASS INIT')
      end
    end

    def register(key, name)
      raise 'Must provide a name' if name.blank? || %w(String Symbol).exclude?(name.class.name)
      raise "#{name} is already registered" if @km_inst.has_key? name

      @km_inst[name] = KMTS.new(key, name)
    end

    def [](name)
      @km_inst[name]
    end
  end

  def initialize(key, name = nil)
    begin
      @key = key
      @name = name
      klass.log_dir_writable?
    rescue Exception => e
      log_error e
    end
  end

  def record(id, action, props={})
    props = klass.hash_keys_to_str(props)
    begin
      return unless is_initialized?
      return set(id, action) if action.class == Hash

      props.update('_n' => action)
      generate_query('e', props, id)
    rescue Exception => e
      log_error e
    end
  end

  def alias(name, alias_to)
    begin
      return unless is_initialized?
      generate_query('a', { '_n' => alias_to, '_p' => name }, false)
    rescue Exception => e
      log_error e
    end
  end

  def set(id, data)
    begin
      return unless is_initialized?
      generate_query('s', data, id)
    rescue Exception => e
      log_error e
    end
  end

  def send_logged_queries # :nodoc:
    line = nil
    begin
      query_log = log_name(:query_old)
      query_log = log_name(:query) unless File.exists?(query_log)
      return unless File.exists?(query_log) # can't find logfile to send
      FileUtils.move(query_log, log_name(:send))
      File.open(log_name(:send)) do |fh|
        while not fh.eof?
          begin
            line = fh.readline.chomp
            send_query line
          rescue Exception => e
            log_query line if line
            log_error e
          end
        end
      end
      FileUtils.rm(log_name(:send))
    rescue Exception => e
      log_error e
    end
  end


  # :stopdoc:
  private

  class << self
    def hash_keys_to_str(hash)
      Hash[*hash.map { |k, v| k.class == Symbol ? [k.to_s,v] : [k,v] }.flatten] # convert all keys to strings
    end

    def log_name(type)
      return @logs[type] if @logs[type]
      fname = ''
      env = environment ? "_#{environment}" : ''
      case type
      when :error
        fname = "kissmetrics#{env}_error.log"
      when :query
        fname = "kissmetrics#{env}_query.log"
      when :query_old # backwards compatibility
        fname = 'kissmetrics_query.log'
      when :sent
        fname = "kissmetrics#{env}_sent.log"
      when :send
        fname = Time.now.to_i.to_s + "kissmetrics_#{env}_sending.log"
      end
      @logs[type] = File.join(@log_dir,fname)
    end

    def log_query(msg, name)
      log(:query, msg, name)
    end

    def log_sent(msg, name)
      log(:sent, msg, name)
    end

    def log_send(msg, name)
      log(:send, msg, name)
    end

    def log_error(error, name)
      if defined?(HoptoadNotifier)
        HoptoadNotifier.notify_or_ignore(KMError.new(error))
      end
      msg = Time.now.strftime('<%c> ') + error.message
      $stderr.puts msg if @to_stderr
      log(:error, msg, name)
      rescue Exception # rescue incase hoptoad has issues
    end

    def send_query(line, name)
      if @dryrun
        log_sent(line, name)
      else
        begin
          host, port = @host.split(':')
          proxy = URI.parse(ENV['http_proxy'] || ENV['HTTP_PROXY'] || '')
          Net::HTTP::Proxy(proxy.host, proxy.port, proxy.user, proxy.password).start(host, port) do |http|
            http.get(line)
          end
        rescue Exception => e
          raise KMError.new("#{e} for host #{@host}")
        end
        log_sent(line, name)
      end
    end

    def log_dir_writable?
      if not FileTest.writable? @log_dir
        $stderr.puts("Could't open #{log_name(:query)} for writing. Does #{@log_dir} exist? Permissions?") if @to_stderr
      end
    end

    private

    def log_dir
      @log_dir
    end

    def host
      @host
    end

    def environment
      @env = Rails.env if defined? Rails
      @env ||= ENV['RACK_ENV']
      @env ||= 'production'
    end

    def log(type, msg, name)
      msg = "#{name}: #{msg}" if name.present?
      begin
        File.open(log_name(type), 'a') do |fh|
          begin
            fh.flock File::LOCK_EX
            fh.puts msg
          ensure
            fh.flock File::LOCK_UN
          end
        end
      rescue Exception => e
        raise KMError.new(e) if type.to_s == 'query'
        # just discard at this point otherwise
      end
    end
  end

  def generate_query(type, data, id = nil)
    data = klass.hash_keys_to_str(data)
    query_arr = []
    query     = ''
    data.update('_p' => id) if id
    data.update('_k' => @key)
    data.update '_d' => 1 if data['_t']
    data['_t'] ||= Time.now.to_i

    unsafe = Regexp.new("[^#{URI::REGEXP::PATTERN::UNRESERVED}]", false, 'N')

    data.inject(query) do |query, key_val|
      query_arr << key_val.collect { |i| URI.escape(i.to_s, unsafe) }.join('=')
    end
    query = '/' + type + '?' + query_arr.join('&')
    if @use_cron
      log_query query
    else
      begin
        send_query query
      rescue Exception => e
        log_query query
        log_error e
      end
    end
  end

  [:log_query, :send_query, :log_error].each do |f|
    define_method f do |msg|
      klass.send(f, msg, @name)
    end
  end

  def is_initialized?
    if @key == nil
      log_error InitError.new('Need to initialize with a valid key')
      return false
    end
    true
  end
  
  def klass
    self.class
  end
  # :startdoc:
end
