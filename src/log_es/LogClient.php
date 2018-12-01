<?php
/**
 * @node_name 日志基础服务
 * Desc: 日志基础服务
 * User: <lideqiang@yundun.com>
 * Date: 2018/9/27 9:47
 *
 * $data = ["key" => "key_0", "name" => "name_0", "title" => "title_0", "create_at" => date("Y-m-d H:i:s")];
 * $jobid = LogClient::instance('only_for_test_1')->add($data);
 */
namespace Jingwu\LogEs;

use Monolog\Logger;
use Monolog\Formatter\LineFormatter;
use Monolog\Formatter\JsonFormatter;

class LogClient extends Core {

    protected $_logkey = '';
    static public $logkeys = [];
    static public $loggers = [];
    static public $instances = [];

    public function __construct($logkey) {
        $this->_logkey = $logkey;
    }

    static public function instance($logkey) {
        if(!isset(self::$instances[$logkey])) self::$instances[$logkey] = new self($logkey);
        return self::$instances[$logkey];
    }

    public function add($row) {
        $result = LogQueue::instance('client')->usePut($this->_logkey, json_encode($row));
        if(!$result) 
            file_put_contents("/tmp/log_queue.log", date("Y-m-d H:i:s")."\t{$this->_logkey}\t".json_encode($row), FILE_APPEND);
        return $result;
    }

    public function debug($msg) {
        self::logger($this->_logkey)->debug($msg);
    }

    public function info($msg) {
        self::logger($this->_logkey)->info($msg);
    }

    public function warn($msg) {
        self::logger($this->_logkey)->warn($msg);
    }

    public function error($msg) {
        self::logger($this->_logkey)->error($msg);
    }

    public function getFields() {
        return self::parseField($this->_logkey);
    }

    static public function parseField($logkey = '') {
        if(!isset(self::$logkeys[$logkey])) {
            $fieldStr = Cfg::instance()->get("logs.{$logkey}");
            if(!$fieldStr) return false;
            self::$logkeys[$logkey] = array_map(function($v) { return trim($v); }, explode(',', $fieldStr));
        }
        return self::$logkeys[$logkey];
    }

    static public function setLogger($logkey, $logger, $level = Logger::DEBUG) {
        $esHandler = new EsHandler($logkey, $level);
        $esHandler->setFormatter(new LineFormatter("[%datetime%] %channel%.%level_name%: %message% %context% %extra% \n", '', true));
        $logger->pushHandler($esHandler);
        self::$loggers[$logkey] = $logger;
    }

    static public function getLogger($logkey) {
        return isset(self::$loggers[$logkey]) ? self::$loggers[$logkey] : false;
    }

    static public function logger($logkey, $level = Logger::DEBUG) {
        if(!isset(self::$loggers[$logkey])) {
            $logger = new Logger($logkey);
            $esHandler = new EsHandler($logkey, $level);
            $esHandler->setFormatter(new LineFormatter("[%datetime%] %channel%.%level_name%: %message% %context% %extra% \n", '', true));
            $logger->pushHandler($esHandler);
            self::$loggers[$logkey] = $logger;
        }
        return self::$loggers[$logkey];
    }
}

