<?php

require 'vendor/autoload.php';

use LogDB\LogDB;

$logDB = new LogDB([
    'url' => 'http://10.60.10.245:16006',
    'token' => 'ynbag3c2zrnhfkalm7yfhcfeu531g77h13rufv34',
]);

$logDB->on('event', function ($event) {
    echo $event->id;
    echo $event->type;
    echo $event->payload;
});

$logDB->listen();



