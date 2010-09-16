# Example: Access to Mio.
require 'rubygems'
require '/home/taro/mio.git/example/memcache.rb'

cache = MemCache.new 'localhost:11211'

cache["hello"] = "world"
cache["intel"] = "cpu"
cache["japan"] = "Tokyo"

print cache["hello"], "\n"
print cache["intel"], "\n"
print cache["japan"], "\n"

print "get_multi:"
p cache.get_multi("mio:range-search", "he", "j", "10", "asc")
