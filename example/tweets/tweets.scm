#!/usr/bin/env mosh
(import (rnrs)
        (mosh control)
        (prefix (mosh cgi) cgi:)
        (only (srfi :1) first last)
        (mosh)
        (uri)
        (mosh file)
        (match)
        (irregex)
        (memcached))

(define header (file->string "/home/taro/mio.git/example/tweets/header.txt"))
(define footer (file->string "/home/taro/mio.git/example/tweets/footer.txt"))


(define (decode text)
  (fold-right (lambda (x y) (irregex-replace/all (cdr x) y (car x)))
        text
        '(("<" . "&lt;")
          (">" . "&gt;")
          ("\"" . "&quot;")
          )))

(define (format-tweet tweet)
  (let1 tweet (irregex-replace/all "@([a-zA-Z]+)" tweet (lambda (m) (format "<a href='http://twitter.com/~a'>~a</a>" (irregex-match-substring m 1)
                                                                            (irregex-match-substring m 0))))
    (irregex-replace/all "#([a-zA-Z]+)" tweet (lambda (m) (format "<a href='http://twitter.com/#search?q=~a'>~a</a>" (uri-encode (irregex-match-substring m 1))
                                                                  (irregex-match-substring m 0))))))

(let-values (([get-parameter get-request-method] (cgi:init)))
  (cgi:header)
  (display header)
  (let* ([mc (memcached-connect "localhost" "11211")]
         [start-key (aif (get-parameter "s") (uri-decode it) "#kosodate")]
         [end-key (aif (get-parameter "e") (uri-decode it) "#kosodate~")]
         [tweet* (memcached-gets mc "mio:range-search" start-key end-key "30" "desc")])
    (format #t "<br><a href='/?s=~a'>Newer</a> | <a href='/?e=~a'>Older</a>" (uri-encode (first (first tweet*))) (uri-encode (first (last tweet*))))
    (for-each (match-lambda
                [(key . (from-user profile-image-url text created-at source))
                 (format #t "
<li>
<div class='list_box'>
  <div class='list_left_wrap'>
    <div class='list_img_wrap'><img src='~a' /></div>
  </div>
  <div class='list_body'>
    <h4>~a</h4>
      <h5>~a<br>
      <span>~a</span></h5>
    </div>
  </div>
</li>" profile-image-url (format-tweet text) (decode source) created-at)]) tweet*)
    (memcached-close mc)
    (format #t "<a href='/?s=~a'>Newer</a> | <a href='/?e=~a'>Older</a>" (uri-encode (first (first tweet*))) (uri-encode (first (last tweet*))))
    (display footer)
    )

)
