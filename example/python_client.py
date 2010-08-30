import memcache

mc = memcache.Client(['localhost:11211'])
mc.set('hello', 'world')
mc.set('intel', 'cpu')
mc.set('japan', 'Tokyo')

print mc.get('hello')
print mc.get('intel')
print mc.get('japan')

# Ooops get_multi doesn't accept response keys which are not appears on search keys.
print mc.get_multi(["mio:range-search", "he", "j", "10", "asc"])


