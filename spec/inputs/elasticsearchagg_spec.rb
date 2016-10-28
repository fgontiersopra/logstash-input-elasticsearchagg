# encoding: utf-8
require "logstash/devutils/rspec/spec_helper"
require "logstash/inputs/elasticsearchagg"
require "elasticsearch"

describe LogStash::Inputs::Elasticsearchagg do

  it_behaves_like "an interruptible input plugin" do
    let(:esclient) { double("elasticsearch-client") }
    let(:config) { { } }

    before :each do
      allow(Elasticsearch::Client).to receive(:new).and_return(esclient)
      hit = {
        "_index" => "logstash-2014.10.12",
        "_type" => "logs",
        "_id" => "C5b2xLQwTZa76jBmHIbwHQ",
        "_score" => 1.0,
        "_source" => { "message" => ["ohayo"] }
      }
      allow(esclient).to receive(:search) { { "hits" => { "hits" => [hit] } } }
      allow(esclient).to receive(:scroll) { { "hits" => { "hits" => [hit] } } }
    end
  end

  it "should retrieve json event from elasticseach" do
    config = %q[
      input {
        elasticsearchagg {
          hosts => ["localhost"]
          index => "mode-*"
          query => '{"size": 0, "query":{"filtered":{"query":{"query_string":{"query":"type:trace","analyze_wildcard":true}},"filter":{"bool":{"must":[{"range":{"@timestamp":{"gte":"now-1M/d","lte":"now/d","format":"epoch_millis"}}}]}}}},"aggs":{"code_tache_agg":{"terms":{"field":"code_tache","size":0,"order":{"_count":"desc"}},"aggs":{"composant_agg":{"terms":{"field":"composant","size":0,"order":{"_count":"desc"}},"aggs":{"code_source_agg":{"terms":{"field":"code_source","size":0,"order":{"_count":"desc"}},"aggs":{"code_caisse_agg":{"terms":{"field":"code_caisse","size":0,"order":{"_count":"desc"}},"aggs":{"code_regime_agg":{"terms":{"field":"code_regime","size":0,"order":{"_count":"desc"}},"aggs":{"code_maille_agg":{"terms":{"field":"code_maille","size":0,"order":{"_count":"desc"}},"aggs":{"date_agg":{"date_histogram":{"field":"@timestamp","interval":"1d","time_zone":"Europe/Berlin","min_doc_count":1,"extended_bounds":{"min":"now-1M/d","max":"now/d"},"offset":"0h"}}}}}}}}}}}}}}} }'
          scroll => '5m'
          
        }
      }
    ]
    
    plugin_response = [{
      "code_tache_agg"=>"EDRT05", 
      "count"=>3583, 
      "composant_agg"=>"MOWD_J", 
      "code_source_agg"=>"Q9", 
      "code_caisse_agg"=>"691", 
      "code_regime_agg"=>"", 
      "code_maille_agg"=>"F", 
      "date_agg"=>1475704800000
      },{
        "code_tache_agg"=>"EDRT05", 
      "count"=>246125, 
      "composant_agg"=>"MOWD_J", 
      "code_source_agg"=>"Q9", 
      "code_caisse_agg"=>"691", 
      "code_regime_agg"=>"", 
      "code_maille_agg"=>"F", 
      "date_agg"=>1475791200000
      }]

    response = {
              "_scroll_id" => "cXVlcnlUaGVuRmV0Y2g",
              "took" => 27,
              "timed_out" => false,
              "_shards" => {
                "total" => 169,
                "successful" => 169,
                "failed" => 0
              },
              "hits" => {
                "total" => 1,
                "max_score" => 1.0,
                "hits" => []
                },
              "aggregations" => {
                "code_tache_agg" => {
                  "doc_count_error_upper_bound" => 0,
                  "sum_other_doc_count" => 0,
                  "buckets" => [ {
                    "key" => "EDRT05",
                    "doc_count" => 4842218,
                    "composant_agg" => {
                      "doc_count_error_upper_bound" => 0,
                      "sum_other_doc_count" => 0,
                      "buckets" => [ {
                        "key" => "MOWD_J",
                        "doc_count" => 4842218,
                        "code_source_agg" => {
                          "doc_count_error_upper_bound" => 0,
                          "sum_other_doc_count" => 0,
                          "buckets" => [ {
                            "key" => "Q9",
                            "doc_count" => 4842218,
                            "code_caisse_agg" => {
                              "doc_count_error_upper_bound" => 0,
                              "sum_other_doc_count" => 0,
                              "buckets" => [ {
                                "key" => "691",
                                "doc_count" => 269545,
                                "code_regime_agg" => {
                                  "doc_count_error_upper_bound" => 0,
                                  "sum_other_doc_count" => 0,
                                  "buckets" => [ {
                                    "key" => "",
                                    "doc_count" => 269545,
                                    "code_maille_agg" => {
                                      "doc_count_error_upper_bound" => 0,
                                      "sum_other_doc_count" => 0,
                                      "buckets" => [ {
                                        "key" => "F",
                                        "doc_count" => 249708,
                                        "date_agg" => {
                                          "buckets" => [ {
                                            "key_as_string" => "2016-10-06T00:00:00.000+02:00",
                                            "key" => 1475704800000,
                                            "doc_count" => 3583
                                          }, {
                                            "key_as_string" => "2016-10-07T00:00:00.000+02:00",
                                            "key" => 1475791200000,
                                            "doc_count" => 246125
                                          } ]
                                        }
                                      }]  
                                    }
                                  }
                                ]}
                              }
                            ]}
                          }
                        ]}
                      }
                    ]}
                  }
                ]}
              }  
            }

    scroll_reponse = {
      "_scroll_id" => "r453Wc1jh0caLJhSDg",
          "took" => 27,
          "timed_out" => false,
          "_shards" => {
          "total" => 0,
          "successful" => 0,
          "failed" => 0
          },
          "hits" => {
            "total" => 1,
            "max_score" => 1.0,
            "hits" => []
            },
          "aggregations" => {}
    }

    client = Elasticsearch::Client.new
    expect(Elasticsearch::Client).to receive(:new).with(any_args).and_return(client)
    expect(client).to receive(:search).with(any_args).and_return(response)
    expect(client).to receive(:scroll).with({ :body => "cXVlcnlUaGVuRmV0Y2g", :scroll=> "5m" }).and_return(scroll_reponse)

    input(config) do |pipeline, queue|
      puts ">>>>>> pipeline #{pipeline}"
      puts ">>>>>> queue #{queue.inspect}"
      insist { queue.size } == 2
      event = queue.pop
      puts "event #{event}"
      insist { event }.is_a?(LogStash::Event)
    end
    
    events.each{|event| puts "event #{event}"}
  end

  # context "with Elasticsearch document information" do
  #   let!(:response) do
  #     {
  #       "_scroll_id" => "cXVlcnlUaGVuRmV0Y2g",
  #       "took" => 27,
  #       "timed_out" => false,
  #       "_shards" => {
  #         "total" => 169,
  #         "successful" => 169,
  #         "failed" => 0
  #       },
  #       "hits" => {
  #         "total" => 1,
  #         "max_score" => 1.0,
  #         "hits" => [ {
  #           "_index" => "logstash-2014.10.12",
  #           "_type" => "logs",
  #           "_id" => "C5b2xLQwTZa76jBmHIbwHQ",
  #           "_score" => 1.0,
  #           "_source" => {
  #             "message" => ["ohayo"],
  #             "metadata_with_hash" => { "awesome" => "logstash" },
  #             "metadata_with_string" => "a string"
  #           }
  #         } ]
  #       }
  #     }
  #   end

  #   let(:scroll_reponse) do
  #     {
  #       "_scroll_id" => "r453Wc1jh0caLJhSDg",
  #       "hits" => { "hits" => [] }
  #     }
  #   end

  #   let(:client) { Elasticsearch::Client.new }

  #   before do
  #     expect(Elasticsearch::Client).to receive(:new).with(any_args).and_return(client)
  #     expect(client).to receive(:search).with(any_args).and_return(response)
  #     allow(client).to receive(:scroll).with({ :body => "cXVlcnlUaGVuRmV0Y2g", :scroll => "1m" }).and_return(scroll_reponse)
  #   end

  #   context 'when defining docinfo' do
  #     let(:config_metadata) do
  #       %q[
  #           input {
  #             elasticsearch {
  #               hosts => ["localhost"]
  #               query => '{ "query": { "match": { "city_name": "Okinawa" } }, "fields": ["message"] }'
  #               docinfo => true
  #             }
  #           }
  #       ]
  #     end

  #     it 'merges the values if the `docinfo_target` already exist in the `_source` document' do
  #       metadata_field = 'metadata_with_hash'

  #       config_metadata_with_hash = %Q[
  #           input {
  #             elasticsearch {
  #               hosts => ["localhost"]
  #               query => '{ "query": { "match": { "city_name": "Okinawa" } }, "fields": ["message"] }'
  #               docinfo => true
  #               docinfo_target => '#{metadata_field}'
  #             }
  #           }
  #       ]

  #       event = input(config_metadata_with_hash) do |pipeline, queue|
  #         queue.pop
  #       end

  #       expect(event.get("[#{metadata_field}][_index]")).to eq('logstash-2014.10.12')
  #       expect(event.get("[#{metadata_field}][_type]")).to eq('logs')
  #       expect(event.get("[#{metadata_field}][_id]")).to eq('C5b2xLQwTZa76jBmHIbwHQ')
  #       expect(event.get("[#{metadata_field}][awesome]")).to eq("logstash")
  #     end

  #     it 'thows an exception if the `docinfo_target` exist but is not of type hash' do
  #       metadata_field = 'metadata_with_string'

  #       config_metadata_with_string = %Q[
  #           input {
  #             elasticsearch {
  #               hosts => ["localhost"]
  #               query => '{ "query": { "match": { "city_name": "Okinawa" } }, "fields": ["message"] }'
  #               docinfo => true
  #               docinfo_target => '#{metadata_field}'
  #             }
  #           }
  #       ]

  #       pipeline = LogStash::Pipeline.new(config_metadata_with_string)
  #       queue = Queue.new
  #       pipeline.instance_eval do
  #         @output_func = lambda { |event| queue << event }
  #       end

  #       expect { pipeline.run }.to raise_error(Exception, /incompatible event/)
  #     end

  #     it "should move the document info to the @metadata field" do
  #       event = input(config_metadata) do |pipeline, queue|
  #         queue.pop
  #       end

  #       expect(event.get("[@metadata][_index]")).to eq('logstash-2014.10.12')
  #       expect(event.get("[@metadata][_type]")).to eq('logs')
  #       expect(event.get("[@metadata][_id]")).to eq('C5b2xLQwTZa76jBmHIbwHQ')
  #     end

  #     it 'should move the document information to the specified field' do
  #       config = %q[
  #           input {
  #             elasticsearch {
  #               hosts => ["localhost"]
  #               query => '{ "query": { "match": { "city_name": "Okinawa" } }, "fields": ["message"] }'
  #               docinfo => true
  #               docinfo_target => 'meta'
  #             }
  #           }
  #       ]
  #       event = input(config) do |pipeline, queue|
  #         queue.pop
  #       end

  #       expect(event.get("[meta][_index]")).to eq('logstash-2014.10.12')
  #       expect(event.get("[meta][_type]")).to eq('logs')
  #       expect(event.get("[meta][_id]")).to eq('C5b2xLQwTZa76jBmHIbwHQ')
  #     end

  #     it "should allow to specify which fields from the document info to save to the @metadata field" do
  #       fields = ["_index"]
  #       config = %Q[
  #           input {
  #             elasticsearch {
  #               hosts => ["localhost"]
  #               query => '{ "query": { "match": { "city_name": "Okinawa" } }, "fields": ["message"] }'
  #               docinfo => true
  #               docinfo_fields => #{fields}
  #             }
  #           }]

  #       event = input(config) do |pipeline, queue|
  #         queue.pop
  #       end

  #       expect(event.get("@metadata").keys).to eq(fields)
  #       expect(event.get("[@metadata][_type]")).to eq(nil)
  #       expect(event.get("[@metadata][_index]")).to eq('logstash-2014.10.12')
  #       expect(event.get("[@metadata][_id]")).to eq(nil)
  #     end
  #   end

  #   context "when not defining the docinfo" do
  #     it 'should keep the document information in the root of the event' do
  #       config = %q[
  #         input {
  #           elasticsearch {
  #             hosts => ["localhost"]
  #             query => '{ "query": { "match": { "city_name": "Okinawa" } }, "fields": ["message"] }'
  #           }
  #         }
  #       ]
  #       event = input(config) do |pipeline, queue|
  #         queue.pop
  #       end

  #       expect(event.get("[@metadata][_index]")).to eq(nil)
  #       expect(event.get("[@metadata][_type]")).to eq(nil)
  #       expect(event.get("[@metadata][_id]")).to eq(nil)
  #     end
  #   end
  # end
end