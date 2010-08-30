<?php
$mem = new Memcached;
$mem->addServer('127.0.0.1', 11211);
$mem->set("hello", "world");
$mem->set("intel", "cpu");
$mem->set("japan", "Tokyo");
printf("%s\n", $mem->get("hello"));
printf("%s\n", $mem->get("intel"));
printf("%s\n", $mem->get("japan"));
var_dump($mem->getMulti(array("mio:range-search", "he", "j", "10", "asc")));
?>
