require 'adapter'
require 'cassandra/0.8'

module Adapter
  module Cassandra
    def key?(key)
      client.exists?(options[:column_family], key_for(key))
    end

    def read(key)
      decode(client.get(options[:column_family], key_for(key)))
    end
    
    def read_many(keys)
      keys = keys.collect { |k| key_for(k)}
      rows = client.multi_get(options[:column_family], keys)
      rows.inject({}) { |result, (k,v)| result.update(k => decode(v)) }
    end

    def write(key, value)
      client.insert(options[:column_family], key_for(key), encode(value))
    end

    def delete(key)
      read(key).tap { client.remove(options[:column_family], key_for(key)) }
    end
    
    def clear
      client.clear_keyspace!
    end

    def encode(value)
      value.inject({}) do |result, (k, v)| 
        case v
        when String
          result.update(k.to_s => v)
        when NilClass # skip nil values. toystore sends them, but cassandra hates 'em.
          result
        else
          result.update(k.to_s => v) # right now we're expecting to get a properly-serialized value...
        end
      end
    end

    def decode(value)
      return nil if value.empty?
      # NOTE: this is super critical here, things break in HORRIBLY MYSTERIOUS ways if we try to
      # pass back an OrderedHash. not really sure why yet.
      value.inject({}) { |result, (k, v)| result.update(k => v) }
    end

  end
end

Adapter.define(:cassandra, Adapter::Cassandra)

