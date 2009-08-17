#!/Users/taro/mosh/mosh
;/usr/local/bin/mosh
(import (rnrs)
        (prefix (mosh cgi) cgi:)
        (mosh)
        (only (srfi :13) string-pad)
        (srfi :2)
        (only (srfi :1) last)
        (only (srfi :19) current-date date->string)
        (memcached))

(include "template/header.scm")
(include "template/article.scm")
(include "template/footer.scm")

(define (profile-image name)
  (define image-alist '(
                        ("higepon"  . "http://friendfeed-media.com/p-31fe497505b0411dbcf9578b8204ccdf-medium-1000")
                        ("akky"     . "http://i.friendfeed.com/p-519a7846f34c11dcb99a003048343a40-medium-1")
                        ("amachang" . "http://i.friendfeed.com/p-a6e5aaeefaf111dca899003048343a40-medium-1")
                        ("kazuho"   . "http://s3.amazonaws.com/twitter_production/profile_images/51501974/close99_bigger.jpg")))
  (cond
   [(assoc name image-alist) => cdr]
   [else "http://friendfeed.com/static/images/nomugshot-medium.png?v=0fa9"]))

(define (make-article-no num)
  (define max-digit 6)
  (string-append "article-" (string-pad (number->string num) max-digit #\0)))

(define max-article-no (make-article-no 999999))
(define min-article-no (make-article-no 0))
(define article-num-page (number->string 3))

(let-values (([get-parameter get-request-method] (cgi:init)))
  (cgi:header)
  (let* ([conn (memcached-connect '(("localhost" . "11211") ("10.89.107.54" . "11211")))]
         [next-article-no (or (memcached-get conn "next-article-no") 1)])
    ;; Header
    (display header)
    ;; Post
    (and-let* ([(eq? 'POST (get-request-method))]
               [name (get-parameter "name")]
               [body (get-parameter "body")])
      (memcached-set! conn (make-article-no next-article-no)
                      0 0 `((article-no . ,next-article-no)
                            (name . ,(cgi:decode name))
                            (body . ,(cgi:decode body))
                            (date . ,(date->string (current-date)))
                            (image . ,(profile-image (cgi:decode name)))))
      ;; ToDo: incr protocol
      (memcached-set! conn "next-article-no" 0 0 (+ next-article-no 1)))

    ;; Delete
    (and-let* ([(eq? 'POST (get-request-method))]
               [(get-parameter "delete")]
               [article-no (get-parameter "article_no")])
      (memcached-delete! conn article-no 0 #f))

    ;; Show articles
    (let* ([from-article-no (or (get-parameter "from-ano") min-article-no)]
           [to-article-no (or (get-parameter "to-ano") max-article-no)]
           [article* (memcached-gets conn "mio:range-search" from-article-no to-article-no article-num-page "desc")]
           [first-article (if (null? article*) #f (car article*))]
           [last-article (if (null? article*) #f (last article*))]
           [prev-required? (> (- (memcached-get conn "next-article-no") 1) (assoc-ref (cdr first-article) 'article-no))])
      (for-each
       (lambda (article)
         (format #t  t-article
                 (assoc-ref (cdr article) 'image)
                 (cgi:escape (assoc-ref (cdr article) 'name))
                 (cgi:escape (assoc-ref (cdr article) 'body))
                 (assoc-ref (cdr article) 'date)
                 (make-article-no (assoc-ref (cdr article) 'article-no))))
       article*)
      (format #t footer (if prev-required?
                            (format "<a style=\"margin:0px\" href=\"?from-ano=~a\">&laquo前の~d件</a> | <a style=\"margin:0px\" href=\"?to-ano=~a\">次の~d件&raquo;</a>"
                                    (car first-article)
                                    article-num-page
                                    (car last-article)
                                    article-num-page)
                            (format "<a style=\"margin:0px\" href=\"?to-ano=~a\">次の~d件&raquo;</a>"
                                    (car last-article)
                                    article-num-page))))))

;; #| apache
;; Listen 8003
;; <VirtualHost *:8003>
;;     DocumentRoot /Users/taro/higepon/mio/example/
;;     AllowEncodedSlashes On
;;     <Directory "/Users/taro/higepon/mio/example/">
;;         DirectoryIndex bbs.scm
;;         Options Indexes FollowSymLinks
;;         AllowOverride None
;;         <FilesMatch ".scm$">
;;         SetHandler cgi-script
;;         </FilesMatch>
;;         Options ExecCGI
;;         Order allow,deny
;;         Allow from all
;;     </Directory>
;;     ErrorLog  /var/log/apache2/bbs.error_log
;;     CustomLog /var/log/apache2/bbs.access_log combined env=!nolog
;; </VirtualHost>
;; #|
