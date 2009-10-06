(use memcache)
(use gauche.test)

(let ([m (guard
          [c (#t (error "can't connect to mio\nTry \n % erlc mio_memcached_compat.erl\n % erl -noshell -s mio start  "))]
          (memcache-connect "localhost" 11211))])
  (let ([oport (open-output-string)]
        [eport (open-output-string)])
    (with-ports #f oport eport
      (lambda ()
        (set m "hello" "world")
        (set m "hi" "japan")
        (set m "ipod" "mp3")
        (test* 'get '((hello . "world")) (get m "hello"))
        (test* 'get '((hello . "world")) (get m "mio:range-search" "he" "hi" "10" "asc"))
        (test* 'get '((hello . "world") (hi . "japan")) (get m "mio:range-search" "he" "ipod" "10" "asc"))
        (test* 'get '((hi . "japan") (hello . "world")) (get m "mio:range-search" "he" "ipod" "10" "desc"))
        (test* 'get '((hi . "japan")) (get m "mio:range-search" "he" "ipod" "1" "desc"))
        (delete m "hello")
        (test* 'get '() (get m "hello"))
        ;; expiry tests
        (set m "abc" "1")
        (set m "abd" "2" :exptime 1)
        (set m "abe" "3")
        (test* 'get '((abd . "2") (abc . "1")) (get m "mio:range-search" "ab" "abe" "2" "desc"))
        (sys-sleep 1)
        (test* 'get '((abc . "1")) (get m "mio:range-search" "ab" "abe" "2" "desc"))
        (memcache-close m)))


    (test-end)))

