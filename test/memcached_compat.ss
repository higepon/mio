(use memcache)
(use gauche.test)

(let ([m (guard
          [c (#t (error "can't connect to mio\nTry \n % erlc mio_memcached_compat.erl\n % erl -noshell -s mio start  "))]
          (memcache-connect "localhost" "11121"))])
  (set m "hello" "world")
  (set m "hi" "japan")
  (test* 'get '((hello . "world")) (get m "hello"))
  (test* 'get '((hello . "world") (hi . "japan")) (get m "mio:range-search" "he" "hi" 10))
  (test* 'get '((hello . "world")) (get m "mio:range-search" "he" "hi" 1))
  (memcache-close m))

