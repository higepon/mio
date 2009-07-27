 (import (rnrs)
        (mosh)
        (only (srfi :1) alist-cons)
        (only (srfi :13) string-join)
        (mosh socket)
        (mosh test)
        (memcached))


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


;; ;; 戻り値を bytevector にする

;; (define response (string->utf8 "VALUE Hello 0 6\r\nWorld!\r\nVALUE Hi 0 6\r\nJapan!\r\nEND\r\n"))


;; #;(define (parse-response response)
;;   (define (get-key start-index)
;;     (let key-loop ([j start-index]
;;                    [value-lst '()])
;;       (let ([ch (integer->char (bytevector-u8-ref response j))])
;;         (cond
;;          [(char=? #\space ch)
;;           (values (list->string (reverse value-lst)) j)]
;;          [else
;;           (key-loop (+ j 1) (cons ch value-lst))]))))
;;   (let ([len (bytevector-length response)])
;;     (let loop ([i 0]
;;                [ret '()])
;;       (cond
;;        [(and ;; ここに length のチェック入れる
;;          (= (bytevector-u8-ref response i)       (char->integer #\V))
;;          (= (bytevector-u8-ref response (+ i 1)) (char->integer #\A))
;;          (= (bytevector-u8-ref response (+ i 2)) (char->integer #\L))
;;          (= (bytevector-u8-ref response (+ i 3)) (char->integer #\U))
;;          (= (bytevector-u8-ref response (+ i 4)) (char->integer #\E))
;;          (= (bytevector-u8-ref response (+ i 5)) (char->integer #\space)))
;;         (let-values (([key next-index] (get-key (+ i 6))))
;;           (display key))
;;         ;; (let key-loop ([j (+ i 6)]
;; ;;                        [value-lst '()])
;; ;;           (let ([ch (integer->char (bytevector-u8-ref response j))])
;; ;;             (cond
;; ;;              [(char=? #\space ch)
;; ;;               (display value-lst)]
;; ;;              [else
;; ;;               (key-loop (+ j 1) (cons ch value-lst))])))
;;         ]
;;        [else
;;         #f]))))

;; #;(define (parse-response res)
;;   (let loop ([i 0]
;;              [ret '()])
;;     ;; END of response?
;;     (if (and (bytevector-u8-ref res i) (char->integer #\E)
;;              (let-values (([token-found? token-start token-end] (token-until-next-char res i #\return)))
;;                (and token-found?
;;                     (bytevector-eqv? res token-start (string->utf8 "END") 0 (string-length "END")))))
;;         (reverse ret)
;;         ;; VALUE
;;         (let-values (([token-found? token-start token-end] (token-until-next-space res i)))
;;           (unless token-found?
;;             (error 'parse-res "malformed response : VALUE expected"))
;;           (unless (bytevector-eqv? res token-start (string->utf8 "VALUE") 0 (string-length "VALUE"))
;;             (error 'parse-res "malformed res : VALUE expected"))
;;           ;; Key
;;           (let-values (([token-found? token-start token-end] (token-until-next-space res (+ token-end 2))))
;;             (unless token-found?
;;               (error 'parse-res "malformed response : Key expected"))
;;             (let ([key (partial-bytevector->string res token-start token-end)])
;;             ;; flags
;;             (let-values (([token-found? token-start token-end] (token-until-next-space res (+ token-end 2))))
;;               (unless token-found?
;;                 (error 'parse-res "malformed response : flags expected"))
;;               ;; length of value terminate with \r\n
;;               (let-values (([token-found? token-start token-end] (token-until-next-char res (+ token-end 2) #\return)))
;;                 (unless token-found?
;;                   (error 'parse-res "malformed response : length of value expected"))
;;                 (let ([value-length (string->number (partial-bytevector->string res token-start token-end))])
;;                   (loop (+ (+ token-end 2 value-length) 3)
;;                         (cons (cons key (partial-bytevector res
;;                                                             (+ token-end 3) ;; skip \n
;;                                                             (+ token-end 2 value-length))) ret)))))))))))



;; (define (token-until-next-space bv start)
;;   (token-until-next-char bv start #\space))

;; (define (token-until-next-char bv start char)
;;   (let ([len (bytevector-length bv)])
;;     (let loop ([i start])
;;       (cond
;;        [(= len i)
;;         ;; End of bytevector
;;         (values #f start (- len 1))]
;;        [(= (bytevector-u8-ref bv i) (char->integer char))
;;         ;; char found
;;         ;; Returns (values found token-start token-end)
;;         (values #t start (- i 1))]
;;        [else
;;         (loop (+ i 1))]))))

;; (define (bytevector-eqv? bv1 bv1-start bv2 bv2-start len)
;;   (let ([len1 (bytevector-length bv1)]
;;         [len2 (bytevector-length bv2)])
;;     (let loop ([i bv1-start]
;;                [j bv2-start])
;;       (cond
;;        [(= len (- i bv1-start)) #t]
;;        [(and (= i len1) (= j len2)) #t]
;;        [(= i len1) #f]
;;        [(= j len2) #f]
;;        [(= (bytevector-u8-ref bv1 i) (bytevector-u8-ref bv2 j))
;;         (loop (+ i 1) (+ j 1))]
;;        [else #f]))))

;; (define (partial-bytevector bv start end)
;;   (let loop ([i start]
;;              [ret '()])
;;     (cond
;;      [(= i (+ end 1))
;;       (u8-list->bytevector (reverse ret))]
;;      [else
;;       (loop (+ i 1) (cons (bytevector-u8-ref bv i) ret))])))


;; (define (partial-bytevector->string bv start end)
;;   (utf8->string (partial-bytevector bv start end)))

;; (test-equal '(#t 0 4)
;;             (let-values (([found start end] (token-until-next-space response 0)))
;;               (list found start end)))

;; (test-true (bytevector-eqv? response 6 (string->utf8 "Hello") 0 5))

;; (test-equal "VALUE" (partial-bytevector->string response 0 4))

;; ;(test-equal `(("Hello" . ,(string->utf8 "World!")) ("Hi" . ,(string->utf8 "Japan!"))) (parse-response response))

(test-results)
