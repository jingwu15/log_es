<?php

namespace Jingwu\LogEs;

use Monolog\Formatter\LineFormatter;
use Monolog\Formatter\JsonFormatter;
use Monolog\Formatter\FormatterInterface;
use Monolog\Handler\AbstractProcessingHandler;

class EsHandler extends AbstractProcessingHandler {

    protected $_logkey = '';
    protected $_queueFile = '';

    public function __construct($logkey) {
        parent::__construct();
        $this->_logkey = $logkey;

        $logdir = Cfg::instance()->get('logdir');
        $this->_queueFile = "{$logdir}/log_queue.log";
    }

    protected function write(array $record) {
        $row = $record;
        $row["create_at"] = $row["datetime"]->format('Y-m-d H:i:s');
        $row["timezone"]  = $row["datetime"]->getTimezone()->getName();
        unset($row['datetime']);
        $body = json_encode($row, JSON_UNESCAPED_UNICODE);
        $result = LogQueue::instance('client')->usePut($this->_logkey, $body);
        if(!$result) 
            file_put_contents($this->_queueFile, date("Y-m-d H:i:s")."\t{$this->_logkey}\t{$body}\n", FILE_APPEND);
    }

    public function setFormatter(FormatterInterface $formatter) {
        return parent::setFormatter($formatter);
    }

}

