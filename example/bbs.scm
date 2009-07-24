 (import (rnrs)
        (mosh)
        (only (srfi :1) alist-cons)
        (only (srfi :13) string-join)
        (mosh socket)
        (mosh test)
        (memcached))

#|
(define (memcached-string-set! conn key value)
  (memcached-set! conn key 0 0 (string->utf8 value)))

(let ([conn (memcached-connect "localhost" "11211")])
  ;; 普通の memcachedd として使う
  (memcached-string-set! conn "Hello" "World!")
  (memcached-string-set! conn "Hi" "Japan!")
  (memcached-string-set! conn  "Hige" "pon")
  (test-equal "World!" (memcached-get conn "Hello"))
  (test-equal "Japan!" (memcached-get conn "Hi"))
  (test-equal "pon" (memcached-get conn "Hige"))
  (test-equal '(("Hello" . "World!") ("Hi" . "Japan!")) (memcached-gets conn "mio:range-search" "He" "Hi" "50"))
)

|#

;; 戻り値を bytevector にする

(define response (string->utf8 "VALUE Hello 0 6\r\nWorld!\r\nVALUE Hi 0 6\r\nJapan!\r\nEND\r\n"))

(define (parse-response response)
  (define (get-key start-index)
    (let key-loop ([j start-index]
                   [value-lst '()])
      (let ([ch (integer->char (bytevector-u8-ref response j))])
        (cond
         [(char=? #\space ch)
          (values (list->string (reverse value-lst)) j)]
         [else
          (key-loop (+ j 1) (cons ch value-lst))]))))
  (let ([len (bytevector-length response)])
    (let loop ([i 0]
               [ret '()])
      (cond
       [(and ;; ここに length のチェック入れる
         (= (bytevector-u8-ref response i)       (char->integer #\V))
         (= (bytevector-u8-ref response (+ i 1)) (char->integer #\A))
         (= (bytevector-u8-ref response (+ i 2)) (char->integer #\L))
         (= (bytevector-u8-ref response (+ i 3)) (char->integer #\U))
         (= (bytevector-u8-ref response (+ i 4)) (char->integer #\E))
         (= (bytevector-u8-ref response (+ i 5)) (char->integer #\space)))
        (let-values (([key next-index] (get-key (+ i 6))))
          (display key))
        ;; (let key-loop ([j (+ i 6)]
;;                        [value-lst '()])
;;           (let ([ch (integer->char (bytevector-u8-ref response j))])
;;             (cond
;;              [(char=? #\space ch)
;;               (display value-lst)]
;;              [else
;;               (key-loop (+ j 1) (cons ch value-lst))])))
        ]
       [else
        #f]))))

(test-equal `(("Hello" . ,(string->utf8 "World!")) ("Hi" . ,(string->utf8 "Japan!"))) (parse-response response))

(test-results)
