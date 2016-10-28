# encoding: utf-8
require "logstash/inputs/base"
require "logstash/namespace"
require "base64"
require "json"
# Read from an Elasticsearch cluster, based on search query results.
# This is useful for replaying test logs, reindexing, etc.
#
# Example:
# [source,ruby]
#     input {
#       # Read all documents from Elasticsearch matching the given query
#       elasticsearch {
#         hosts => "localhost"
#         query => '{ "query": { "match": { "statuscode": 200 } }, "sort": [ "_doc" ] }'
#       }
#     }
#
# This would create an Elasticsearch query with the following format:
# [source,json]
#     curl 'http://localhost:9200/logstash-*/_search?&scroll=1m&size=1000' -d '{
#       "query": {
#         "match": {
#           "statuscode": 200
#         }
#       },
#       "sort": [ "_doc" ]
#     }'
#



class LogStash::Inputs::Elasticsearchagg < LogStash::Inputs::Base
  config_name "elasticsearchagg"

  default :codec, "json"

  # List of elasticsearch hosts to use for querying.
  # each host can be either IP, HOST, IP:port or HOST:port
  # port defaults to 9200
  config :hosts, :validate => :array

  # The index or alias to search.
  config :index, :validate => :string, :default => "logstash-*"

  # The query to be executed. Read the Elasticsearch query DSL documentation
  # for more info
  # https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl.html
  config :query, :validate => :string, :default => '{ "sort": [ "_doc" ] }'

  # This allows you to set the maximum number of hits returned per scroll.
  config :size, :validate => :number, :default => 1000

  # This parameter controls the keepalive time in seconds of the scrolling
  # request and initiates the scrolling process. The timeout applies per
  # round trip (i.e. between the previous scroll request, to the next).
  config :scroll, :validate => :string, :default => "1m"

  # If set, include Elasticsearch document information such as index, type, and
  # the id in the event.
  #
  # It might be important to note, with regards to metadata, that if you're
  # ingesting documents with the intent to re-index them (or just update them)
  # that the `action` option in the elasticsearch output wants to know how to
  # handle those things. It can be dynamically assigned with a field
  # added to the metadata.
  #
  # Example
  # [source, ruby]
  #     input {
  #       elasticsearch {
  #         hosts => "es.production.mysite.org"
  #         index => "mydata-2018.09.*"
  #         query => "*"
  #         size => 500
  #         scroll => "5m"
  #         docinfo => true
  #       }
  #     }
  #     output {
  #       elasticsearch {
  #         index => "copy-of-production.%{[@metadata][_index]}"
  #         index_type => "%{[@metadata][_type]}"
  #         document_id => "%{[@metadata][_id]}"
  #       }
  #     }
  #
  config :docinfo, :validate => :boolean, :default => false

  # Where to move the Elasticsearch document information by default we use the @metadata field.
  config :docinfo_target, :validate=> :string, :default => LogStash::Event::METADATA

  # List of document metadata to move to the `docinfo_target` field
  # To learn more about Elasticsearch metadata fields read
  # http://www.elasticsearch.org/guide/en/elasticsearch/guide/current/_document_metadata.html
  config :docinfo_fields, :validate => :array, :default => ['_index', '_type', '_id']

  # Basic Auth - username
  config :user, :validate => :string

  # Basic Auth - password
  config :password, :validate => :password

  # SSL
  config :ssl, :validate => :boolean, :default => false

  # SSL Certificate Authority file
  config :ca_file, :validate => :path

  def register
    require "elasticsearch"

    @options = {
      :index => @index,
      :body => @query,
      :scroll => @scroll,
      :size => @size
    }

    transport_options = {}

    if @user && @password
      token = Base64.strict_encode64("#{@user}:#{@password.value}")
      transport_options[:headers] = { :Authorization => "Basic #{token}" }
    end

    hosts = if @ssl then
      @hosts.map do |h|
        host, port = h.split(":")
        { :host => host, :scheme => 'https', :port => port }
      end
    else
      @hosts
    end

    if @ssl && @ca_file
      transport_options[:ssl] = { :ca_file => @ca_file }
    end

    @client = Elasticsearch::Client.new(:hosts => hosts, :transport_options => transport_options)
  end

  def run(output_queue)
    # get first wave of data
    r = @client.search(@options)
    data = {}
    has_aggs = r['aggregations'] != nil && r['aggregations'].any?
    if has_aggs
      r['aggregations'].each do |aggregation| 
        puts "===================================="
        push_aggregation(aggregation, data, output_queue) 
        # puts "data.length : #{data.length}"
      end
    end
    puts "[[[[[[[[[[[[[[[[[]]]]]]]]]]]]]]]]]]]"
    
    puts "r #{r}"
    #stop = !(r['aggregations'].length > 0)
    #puts "stop #{stop}"
    while has_aggs && !stop?
      r = process_next_scroll(output_queue, r['_scroll_id'])
      puts "has_aggs #{has_aggs}"
      stop = !(r['aggregations'].length > 0)
      puts "stop #{stop}"
    end
  end

  private

  def process_next_scroll(output_queue, scroll_id)
    r = scroll_request(scroll_id)
    r['aggregations'].each { |aggregation| push_aggregation(aggregation) }
    {'has_agg' => r['aggregations'].any?, '_scroll_id' => r['_scroll_id']}
  end

  # bucket is compound of
  # - key property (value of aggrégation)
  # - number of doc
  # - aggregation object
  def push_bucket(bucket, data, aggname, output_queue)
    #puts "> push_bucket #{bucket}"
    has_sub_agg = false
    bucket.each do |entry|
      
      # manage data
      # puts"data.length #{data.length}"
      # initialize data
      if data.length == 0 
        # puts "initialize data"
        data = Hash.new
        # puts "data.length #{data.length}"
      end
      

      # manage entry
      # puts "entry #{entry}" 
      # puts "test entry[0] #{entry[0]}"  
      if entry[0] == "key"
        aggvalue = entry[1]
        # puts "merging data for key"
        data = data.merge({ aggname => aggvalue })
      end
      if entry[0] == "doc_count"
        nbdocs = entry[1] 
        # puts "merging data for doc_cout"
        data = data.merge({ "count" => nbdocs })
      end
    
      
      # puts"data.length #{data.length}"
      #aggregation
      if (entry[1]).is_a?(Hash)
        # puts "Hash in buckets => agregation"
        has_sub_agg = true
        push_aggregation(entry, data, output_queue)
      else
        puts "entry[1] is not Hash #{entry[1]}"
      end
      
    end

    #
    if ! has_sub_agg 
      puts "cannot find hash = > end of embeded aggregation , convert data to event"
      puts "data #{data}"
      puts "data.to_json #{data.to_json}"
      event = LogStash::Event.new(data)
      puts "event #{event.to_json}"
      puts "decorate"
      decorate(event)
      puts "event #{event.to_json}"
      puts "push to output_queue"
      output_queue << event
      puts "done"
    end
  end 
  # aggregation is compounded of
  #  - aggregationj name
  #  - array with buckets array 

  def push_aggregation(aggregation, data, output_queue)
    #puts "> push_aggregation #{aggregation}"

    if aggregation != nil && aggregation.length == 2
      aggname = aggregation[0]
      aggentry = aggregation[1]

      #puts"inside aggregation #{aggentry}"
      #test if have children
      if aggentry["buckets"] != nil?
        puts "has buckets"
        aggentry["buckets"].each do |bucket| 
          puts "treat bucket #{bucket}"
          push_bucket(bucket, data, aggname, output_queue)
        end 
      else
        puts "no buckets"
      end  

    end
  end


  # def push_hit(hit, output_queue)
  #   event = LogStash::Event.new(hit['_source'])
  #   decorate(event)

  #   if @docinfo
  #     # do not assume event[@docinfo_target] to be in-place updatable. first get it, update it, then at the end set it in the event.
  #     docinfo_target = event.get(@docinfo_target) || {}

  #     unless docinfo_target.is_a?(Hash)
  #       @logger.error("Elasticsearch Input: Incompatible Event, incompatible type for the docinfo_target=#{@docinfo_target} field in the `_source` document, expected a hash got:", :docinfo_target_type => docinfo_target.class, :event => event)

  #       # TODO: (colin) I am not sure raising is a good strategy here?
  #       raise Exception.new("Elasticsearch input: incompatible event")
  #     end

  #     @docinfo_fields.each do |field|
  #       docinfo_target[field] = hit[field]
  #     end

  #     event.set(@docinfo_target, docinfo_target)
  #   end

  #   output_queue << event
  # end

  def scroll_request scroll_id
    @client.scroll(:body => scroll_id, :scroll => @scroll)
  end
end
