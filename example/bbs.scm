#!/usr/local/bin/mosh
(import (rnrs)
        (prefix (mosh cgi) cgi:)
        (mosh)
        (memcached))

(let ([conn (memcached-connect "localhost" "11211")])
  (memcached-set! conn "bbs1" 0 0 '((name . "higepon") (body . "hello")))
  (memcached-set! conn "bbs2" 0 0 '((name . "higepon") (body . "hello2")))

(cgi:header)
(display "
<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Strict//EN\" \"http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd\">
<html xmlns=\"http://www.w3.org/1999/xhtml\">
<head>
<meta http-equiv=\"Content-Type\" content=\"text/html; charset=UTF-8\"/>
<title>Mio BBS</title>
<link rel=\"stylesheet\" href=\"mio.css\" type=\"text/css\"/>
<body >
<div id=\"bodydiv\">
<div id=\"container\">
<div id=\"header\">
<table>
  <tr>
    <td class=\"logo\"><a href=\"/\"><img src=\"logo.png\" alt=\"Logo\" style=\"\"/></a></td>
    <td><table>
      <tr>
        <td>
        </td>
        <td id=\"extralinks\">
        </td>

      </tr>
    </table>
  </td>
</tr>
</table>
</div>
<div id=\"sidebar\">
<div id=\"profile\">
<table>
  <tr>
    <td class=\"image\"><a href=\"/higepon\" sid=\"31fe497505b0411dbcf9578b8204ccdf\" class=\"l_profile\"><img src=\"http://i.friendfeed.com/p-31fe497505b0411dbcf9578b8204ccdf-medium-1\" alt=\"higepon\" class=\"picture medium\"/></a></td>
    <td class=\"body\">
      <div class=\"name\"><a href=\"/higepon\" sid=\"31fe497505b0411dbcf9578b8204ccdf\" class=\"l_profile\">higepon</a></div>
      <div>

      </div>
    </td>
  </tr>
</table>
</div>
</div>
<div id=\"body\">
<div class=\"box white\" id=\"page\">
<div class=\"box-bar home\">
<div class=\"box-corner\"></div>
<div class=\"box-bar-text\">
<div id=\"realtimectl\" class=\"control\"><div class=\"l_realtimepause\" title=\"更新を一時停止\"></div></div>
<div id=\"realtimestatus\" class=\"control\"></div>
<span class=\"title\"><a href=\"/\">ホーム</a></span>
</div>
</div>
<div class=\"subbar home\" style=\"display:none\"></div>
<div class=\"box-body\">
<div class=\"sharebox\">
<form action=\"/a/share\" method=\"post\" enctype=\"multipart/form-data\" onsubmit=\"return shareSubmit($(this))\">
<div class=\"to\">
<table>
  <tr>
    <td>To:&nbsp;</td>
    <td class=\"tobody\">
      <ul class=\"l_tolist\">
        <li class=\"spacer\"></li>

        <li class=\"l_tocard public\" sid=\"higepon+friends\">自分 <img src=\"/static/images/to-delete.png?v=f1c8\" class=\"l_toremove\" title=\"削除\"/></li>

        <li class=\"addedit\"><a href=\"#\">追加/編集</a></li>
        <input type=\"text\" class=\"l_toinput\" maxlength=\"50\"/>
      </ul>
    </td>
  </tr>
</table>
</div>
<div class=\"title\"><div class=\"textbox\"><textarea name=\"title\" class=\"title\" rows=\"2\" cols=\"40\"></textarea></div></div>
<div class=\"progress\"><table></table></div>
<div class=\"files\"></div>
<div class=\"ops\">
<table>
  <tr>
    <td class=\"attach\">
    </td>
    <td class=\"button\">
      <table>
        <tr>
          <td class=\"cc\">
          </td>
          <td class=\"post\">
            <input type=\"submit\" value=\"投稿\"/>
            <input type=\"hidden\" name=\"next\" value=\"/\"/>
            <input type=\"hidden\" name=\"streams\" value=\"higepon\"/>
          </td>
        </tr>
      </table>
    </td>
  </tr>
</table>
</div>
</form>
</div>")
(let ([article* (memcached-gets conn "mio:range-search" "bbs1" "bbs99999" "10")])
(for-each
 (lambda (article)
(format #t "
<div id=\"feed\">
<div class=\"l_entry entry\" id=\"e-297714c267d62f15935dfe16bec7bb19\" eid=\"297714c267d62f15935dfe16bec7bb19\">
<div class=\"profile\">
<a href=\"/mala\" sid=\"a8f43e08005b11dd83a2003048343a40\" class=\"l_profile\"><img src=\"http://i.friendfeed.com/p-519a7846f34c11dcb99a003048343a40-medium-1\" alt=\"Akky AKIMOTO\" class=\"picture medium\"/></a>
</div>
<div class=\"body\">
<div class=\"ebody\">
<div class=\"title\">
<div class=\"name\">
<a href=\"/mala\" sid=\"a8f43e08005b11dd83a2003048343a40\" class=\"l_profile\">~a</a>
</div>
<div class=\"text\">
~a
</div>
</div>
</div>
<div class=\"info\">
<a class=\"date\" href=\"/mala/297714c2/bash-cgi-jsessionid\">4 分前</a>
<a class=\"service\" rel=\"nofollow\" href=\"http://twitter.com/bulkneets/statuses/2795192602\">Twitter</a> 経由
</div>
</div>
<div class=\"clear\"></div>
</div>
" (assoc-ref article 'name) (assoc-ref article 'body)))
article*))


(display "
<div id=\"feed\">
<div class=\"l_entry entry\" id=\"e-297714c267d62f15935dfe16bec7bb19\" eid=\"297714c267d62f15935dfe16bec7bb19\">
<div class=\"profile\">
<a href=\"/mala\" sid=\"a8f43e08005b11dd83a2003048343a40\" class=\"l_profile\"><img src=\"http://i.friendfeed.com/p-519a7846f34c11dcb99a003048343a40-medium-1\" alt=\"Akky AKIMOTO\" class=\"picture medium\"/></a>
</div>
<div class=\"body\">
<div class=\"ebody\">
<div class=\"title\">
<div class=\"name\">
<a href=\"/mala\" sid=\"a8f43e08005b11dd83a2003048343a40\" class=\"l_profile\">mala</a>
</div>
<div class=\"text\">
bash + CGI で動いているのをわざわざヘッダ偽装したりJSESSIONID吐いたりとかしないと思いますよ
</div>
</div>
</div>
<div class=\"info\">
<a class=\"date\" href=\"/mala/297714c2/bash-cgi-jsessionid\">4 分前</a>
<a class=\"service\" rel=\"nofollow\" href=\"http://twitter.com/bulkneets/statuses/2795192602\">Twitter</a> 経由
</div>
</div>
<div class=\"clear\"></div>
</div>
</div>
<div class=\"pager bottom\">
<a style=\"margin:0px\" href=\"/summary/1\">前の10件</a> -
<a href=\"/?start=30\">&raquo;</a>
</div>
<div style=\"clear:both; height:12px\"></div>
</div>
<div class=\"box-bottom\">
<div class=\"box-corner\"></div>
<div class=\"box-spacer\"></div>
</div>
</div>
</div>
<div id=\"footer\">
&copy;2009 Cybozu labs.
</div>
</div>
</div><div id=\"extradiv\"></div><div id=\"extradivtoo\"></div>
</body>
</html>
")
)
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
