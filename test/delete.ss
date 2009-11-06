(use memcache)
(use gauche.test)

(let ([m (guard
          [c (#t (error "can't connect to mio\nTry \n % erlc mio_memcached_compat.erl\n % erl -noshell -s mio start  "))]
          (memcache-connect "localhost" 11211))])
  (do ([i 0 (+ i 1)])
      ((= i 999))
    (display (delete m (number->string i))))
  (memcache-close m))

