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
    static public $_logkeys = [];
    static public $_loggers = [];
    static public $_instances = [];

    public function __construct($logkey) {
        $this->_logkey = $logkey;
    }

    static public function instance($logkey) {
        if(!isset(self::$_instances[$logkey])) {
            self::$_instances[$logkey] = new self($logkey);
        }
        return self::$_instances[$logkey];
    }

    public function add($row) {
        $keys = array_keys($row);
        $fields = self::parseField($this->_logkey);
        if($keys == $fields) {
            $result = LogQueue::instance('client')->usePut($this->_logkey, json_encode($row));
        } else {
            //字段不匹配，异常
            //$result = LogQueue::instance('client')->usePut($this->_logkey, $row);
            $result = false;
        }
        return $result;
    }

    public function error($msg) {
        self::logger($this->_logkey)->info($msg);
    }

    public function debug($msg) {
        self::logger($this->_logkey)->info($msg);
    }

    public function warn($msg) {
        self::logger($this->_logkey)->info($msg);
    }

    public function info($msg) {
        self::logger($this->_logkey)->info($msg);
    }

    public function getFields() {
        return self::parseField($this->_logkey);
    }

    static public function parseField($logkey = '') {
        if(!isset(self::$_logkeys[$logkey])) {
            $fieldStr = Cfg::instance()->get("logs.{$logkey}");
            if(!$fieldStr) return false;
            self::$_logkeys[$logkey] = array_map(function($v) { return trim($v); }, explode(',', $fieldStr));
        }
        return self::$_logkeys[$logkey];
    }

    static public function setLogger($logkey, $logger, $level = Logger::DEBUG) {
        $esHandler = new EsHandler($logkey, $level);
        $esHandler->setFormatter(new LineFormatter("[%datetime%] %channel%.%level_name%: %message% %context% %extra% \n", '', true));
        $logger->pushHandler($esHandler);
        self::$_loggers[$logkey] = $logger;
    }

    static public function getLogger($logkey) {
        return isset(self::$_loggers[$logkey]) ? self::$_loggers[$logkey] : false;
    }

    static public function logger($logkey, $level = Logger::DEBUG) {
        if(!isset(self::$_loggers[$logkey])) {
            $logger = new Logger($logkey);
            $esHandler = new EsHandler($logkey, $level);
            $esHandler->setFormatter(new LineFormatter("[%datetime%] %channel%.%level_name%: %message% %context% %extra% \n", '', true));
            $logger->pushHandler($esHandler);
            self::$_loggers[$logkey] = $logger;
        }
        return self::$_loggers[$logkey];
    }
}

