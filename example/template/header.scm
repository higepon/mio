(define header "
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
    <td class=\"logo\"><a href=\"/\"></a></td>
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
</div>
</div>
<div id=\"body\">
<div class=\"box white\" id=\"page\">
<div class=\"box-bar home\">
<div class=\"box-corner\"></div>
<div class=\"box-bar-text\">
<div id=\"realtimectl\" class=\"control\"><div class=\"l_realtimepause\" title=\"更新を一時停止\"></div></div>
<div id=\"realtimestatus\" class=\"control\"></div>
<span class=\"title\"><a href=\"/\">Mio サンプル掲示板</a></span>
</div>
</div>
<div class=\"subbar home\" style=\"display:none\"></div>
<div class=\"box-body\">
<div class=\"sharebox\">
<form action=\"/\" method=\"post\">
<div class=\"to\">
</div>
<div class=\"title\"><div class=\"textbox\"><textarea name=\"body\" class=\"title\" rows=\"2\" cols=\"40\"></textarea></div></div>
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
            名前:<input type=\"text\" name=\"name\" value=\"higepon\"/>
            <input type=\"submit\" value=\"投稿\"/>

          </td>
        </tr>
      </table>
    </td>
  </tr>
</table>
</div>
</form>
</div>")
