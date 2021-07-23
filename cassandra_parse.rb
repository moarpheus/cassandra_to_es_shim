require 'cassandra'
require 'byebug'
require "chronic"
require "elasticsearch"
require "date"

cluster = Cassandra.cluster(username: 'xxxx', password: 'xxxx', hosts: ['xxxx'], port: 9042)

session  = cluster.connect('xxx')

query_string = "SELECT * FROM xxx"

results = session.execute(query_string)

es_client = Elasticsearch::Client.new host: "http://localhost:9200"

es_bulk = []
results.each do |entry|

  result = {
    "date" => Time.now.strftime("%FT%TZ"),
    "xxx" => entry["xxx"]
  }

  puts result

  es_bulk << {
    index: {
      _index: "xxx-#{time_start.strftime('%Y.%m.%d')}",
      _type: "_doc",
      data:   result,
    },
  }

end

puts "Enriched the results"

es_bulk.each_slice(50_000) do |slice|
  es_client.bulk body: slice
end

puts "Sent results to ES"
