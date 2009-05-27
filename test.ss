(use memcache)
(use gauche.test)

(let ([m (guard
          [c (#t (error "can't connect to mio\nTry \n % erlc mio.erl\n % erl -noshell -s mio start  "))]
          (memcache-connect "localhost" "11211"))])
  (set m "hello" "world")
  (test* 'get '((hello . "world")) (get m "hello"))
  (memcache-close m))

