 (import (rnrs)
        (mosh)
        (only (srfi :1) alist-cons)
        (only (srfi :13) string-join)
        (mosh socket)
        (mosh test))

(define-record-type memcached-client
  (fields
   (immutable socket)))

(define (memcache-connect server port)
  (make-memcached-client (make-client-socket server port)))

(define (memcache-recv conn)
  (let ([buffer-size 4096]
        [socket (memcached-client-socket conn)])
      (let loop ([ret (make-bytevector 0)]
                 [data (socket-recv socket buffer-size)])
        (let* ([total-size (+ (bytevector-length ret) (bytevector-length data))]
               [new (make-bytevector total-size)])
          (bytevector-copy! ret 0 new 0 (bytevector-length ret))
          (bytevector-copy! data 0 new (bytevector-length ret) (bytevector-length data))
          (if (= (bytevector-length data) buffer-size)
              (loop new (socket-recv socket buffer-size))
              new)))))

(define (memcache-send conn text)
  (socket-send (memcached-client-socket conn) (string->utf8 text)))

(define (memcache-set! conn key value)
    (memcache-send conn (format "set ~a 0 0 ~d\r\n~a\r\n" key (string-length value) value))
    (memcache-recv conn))

(define (memcache-set/s! conn key value index)
  (memcache-send conn (format "set/s ~a ~a 0 0 ~d\r\n~a\r\n" key index (string-length value) value))
  (memcache-recv conn))

(define (memcache-get/s conn key1 key2 index limit)
  (memcache-send conn (format "get/s ~a ~a ~a ~d\r\n" key1 key2 index limit))
    (let loop ([lines (string-split (utf8->string (memcache-recv conn)) #\newline)]
               [ret '()])
      (cond
       [(#/^VALUE ([^\s]+) [^\s]+ \d+$/ (car lines)) =>
        (lambda (m)
          (loop (cddr lines) (alist-cons (m 1) (cadr lines) ret)))]
       [(#/END/ (car lines))
        (reverse ret)]
       [else
        (error 'memcach-get/s "malformed gets replry" (car lines))])))

(define (memcache-gets conn . keys)
  (memcache-send conn (format "get ~a\r\n" (string-join keys " ")))
    (let loop ([lines (string-split (utf8->string (memcache-recv conn)) #\newline)]
               [ret '()])
      (format (current-error-port) "line= ~a\n" lines)
      (cond
       [(#/^VALUE ([^\s]+) [^\s]+ \d+$/ (car lines)) =>
        (lambda (m)
          (loop (cddr lines) (alist-cons (m 1) (cadr lines) ret)))]
       [(#/END/ (car lines))
        (reverse ret)]
       [else
        (error 'memcach-get/s "malformed gets replry" (car lines))])))

(define (memcache-get conn key)
  (let ([ret (assoc key (memcache-gets conn key))])
    (if ret
        (cdr ret)
        #f)))

(let ([conn (memcache-connect "localhost" "11211")])
  ;; 普通の memcached として使う
  (memcache-set! conn "Hello" "World!")
  (memcache-set! conn "Hi" "Japan!")
  (memcache-set! conn  "Hige" "pon")
  (test-equal "World!" (memcache-get conn "Hello"))
;;   (test-equal "Bye!" (memcache-get conn "Good"))
;;   (test-equal "World!" (memcache-get conn "Hello")))
)

(test-results)
