<?php

namespace Jingwu\LogEs;

use Monolog\Formatter\LineFormatter;
use Monolog\Formatter\JsonFormatter;
use Monolog\Formatter\FormatterInterface;
use Monolog\Handler\AbstractProcessingHandler;

class EsHandler extends AbstractProcessingHandler {

    protected $_logkey = '';
    protected $_queueFile = '';
    protected $_bodySizeMax = 0;

    public function __construct($logkey) {
        parent::__construct();
        $this->_logkey = $logkey;

        $logdir = Cfg::instance()->get('logdir');
        $logpre = Cfg::instance()->get('logpre');
        $this->_queueFile = "{$logdir}/logauto_{$logpre}.log";
        $this->_bodySizeMax = Cfg::instance()->get('limit.body_size_max');
    }

    protected function write(array $record) {
        $logkey = Cfg::instance()->get('logpre').$this->_logkey;
        $row = $record;
        $row["create_at"] = $row["datetime"]->format('Y-m-d H:i:s');
        $row["timezone"]  = $row["datetime"]->getTimezone()->getName();
        unset($row['datetime']);
        $body = json_encode($row, JSON_UNESCAPED_UNICODE);
        //如果消息超长，则写文件，而不写入ES
        if(strlen($body) > $this->_bodySizeMax) {
            file_put_contents($this->_queueFile, date("Y-m-d H:i:s")."\t{$logkey}\t{$body}\n", FILE_APPEND);
        } else {
            $result = LogQueue::instance('client')->usePut($logkey, $body);
            $flag = 1;
            if(!$result) {
                $result = LogQueue::instance('client')->reconnect();
                if($result) {
                    $result = LogQueue::instance('client')->usePut($logkey, $body);
                    if(!$result) $flag = 0;
                } else {
                    $flag = 0;
                }
            }
            //执行失败, 写入文件
            if($flag === 0) file_put_contents($this->_queueFile, date("Y-m-d H:i:s")."\t{$logkey}\t{$body}\n", FILE_APPEND);
        }
    }

    public function setFormatter(FormatterInterface $formatter) {
        return parent::setFormatter($formatter);
    }

}

