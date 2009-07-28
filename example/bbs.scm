#!/Users/taro/mosh/mosh
;/usr/local/bin/mosh
(import (rnrs)
        (prefix (mosh cgi) cgi:)
        (mosh)
        (memcached))

(include "template/header.scm")
(include "template/article.scm")
(include "template/footer.scm")
  (cgi:header)
(let-values (([get-parameter get-request-method] (cgi:init)))

  (let* ([conn (memcached-connect "localhost" "11211")]
         [next-article-no (or (memcached-get conn "next-article-no") 5)])

    (memcached-set! conn "bbs1" 0 0 '((name . "higepon")
                                      (body . "例が載っているが、z は mpz_init(z) と初期化しておく必要がある。ML でも指摘されているのだけどマニュアルが不親切なので注意。")))
    (memcached-set! conn "bbs2" 0 0 '((name . "higepon")
                                      (body . "hello2")))


    (display header)
    (when (eq? 'POST (get-request-method))
      (format #t "higehige[~a][~a]\n" (get-parameter "body") (cgi:decode (get-parameter "body")))
      (memcached-set! conn (format "bbs~d" next-article-no)
                      0 0 `((name . ,(cgi:decode (get-parameter "name")))
                            (body . ,(cgi:decode (get-parameter "body"))))))

    (let ([article* (memcached-gets conn "mio:range-search" "bbs1" "bbs99999" "10")])
      (for-each
       (lambda (article)
         (format #t  t-article (assoc-ref article 'name) (assoc-ref article 'body)))
       article*))
    (display footer)))
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
