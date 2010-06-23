#!/home/taro/mosh.git/mosh
(import (rnrs)
        (mosh control)
        (prefix (mosh cgi) cgi:)
        (mosh)
        (mosh file)
        (match)
        (memcached))

(define header (file->string "/home/taro/mio.git/example/tweets/header.txt"))
(define footer (file->string "/home/taro/mio.git/example/tweets/footer.txt"))

(define (decode text)
  (fold-right (lambda (x y) (regexp-replace-all (cdr x) y (car x)))
        text
        '(("<" . #/&lt;/)
          (">" . #/&gt;/)
          ("\"" .#/&quot;/)
          )))

(let-values (([get-parameter get-request-method] (cgi:init)))
  (cgi:header)
  (display header)
  (let1 mc (memcached-connect "localhost" "11211")
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
      <h5><~a<br>
      <span>~a</span></h5>
    </div>
  </div>
</li>" profile-image-url text (decode source) created-at)])
              (memcached-gets mc "mio:range-search" "#kosodate" "#kosodatf" "10" "desc"))
    (memcached-close mc))
  (display footer)
)
