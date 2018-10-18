<?php

namespace Jingwu\LogEs;

use Monolog\Handler\AbstractProcessingHandler;
use Monolog\Formatter\LineFormatter;
use Monolog\Formatter\JsonFormatter;

class EsHandler extends AbstractProcessingHandler {

    protected $_logkey = '';

    public function __construct($logkey) {
        parent::__construct();
        $this->_logkey = $logkey;
    }

    protected function write(array $record) {
        $row = $record;
        $row["create_at"] = $row["datetime"]->format('Y-m-d H:i:s');
        $row["timezone"]  = $row["datetime"]->getTimezone()->getName();
        unset($row['datetime']);
        $result = LogQueue::instance('client')->usePut($this->_logkey, json_encode($row));
    }

    public function setFormatter($formatter) {
        return parent::setFormatter($formatter);
    }

}

