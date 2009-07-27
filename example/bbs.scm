 (import (rnrs)
        (mosh)
        (only (srfi :1) alist-cons)
        (only (srfi :13) string-join)
        (mosh socket)
        (mosh test)
        (memcached))

;; we assume utf8
(define (memcached-string-set! conn key value)
  (memcached-set! conn key 0 0 (string->utf8 value)))

(define (memcached-string-get conn key)
  (utf8->string (memcached-get conn key)))

(define (memcached-string-gets conn . key*)
  (map (lambda (key-value) (cons (car key-value) (utf8->string (cdr key-value))))
       (apply memcached-gets conn key*)))

(let ([conn (memcached-connect "localhost" "11211")])
  ;; 普通の memcachedd として使う
  (memcached-string-set! conn "Hello" "World!")
  (memcached-string-set! conn "Hi" "Japan!")
  (memcached-string-set! conn  "Hige" "pon")
  (test-equal "World!" (memcached-string-get conn "Hello"))
  (test-equal "Japan!" (memcached-string-get conn "Hi"))
  (test-equal "pon" (memcached-string-get conn "Hige"))
  (test-equal '(("Hello" . "World!") ("Hi" . "Japan!")) (memcached-string-gets conn "mio:range-search" "He" "Hi" "50"))
)
(test-results)
