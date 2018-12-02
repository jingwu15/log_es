<?php
/**
 * Desc: 日志基础服务
 * User: <lideqiang@yundun.com>
 * Date: 2018/9/27 9:47
 *
 * $data = ["key" => "key_0", "name" => "name_0", "title" => "title_0", "create_at" => date("Y-m-d H:i:s")];
 * $jobid = LogClient::instance('only_for_test_1')->add($data);
 */
namespace Jingwu\LogEs;

use Monolog\Logger;
use Monolog\Handler\StreamHandler;
use Monolog\Formatter\LineFormatter;
use Monolog\Formatter\JsonFormatter;

class LogClient extends Core {

    const DEBUG    = 100;        //Logger::DEBUG
    const INFO     = 200;        //Logger::INFO
    const NOTICE   = 250;        //Logger::NOTICE
    const WARNING  = 300;        //Logger::WARNING
    const ERROR    = 400;        //Logger::ERROR
    const CRITICAL = 500;        //Logger::CRITICAL

    protected $_logkey       = '';
    protected $_level        = Logger::DEBUG;
    protected $_useEs        = true;
    protected $_useFile      = false;
    protected $_useStdout    = false;
    protected $_queuefile    = '';
    static public $loggers   = [];
    static public $instances = [];

    public function __construct($logkey) {
        $this->_logkey = $logkey;
        $logdir = Cfg::instance()->get('logdir');
        $this->_queueFile = "{$logdir}/log_queue.log";
    }

    static public function instance($logkey = 'default') {
        $logkey = trim($logkey);
        if(empty($logkey)) throw new Exception('logkey is empty');

        if(!isset(self::$instances[$logkey])) self::$instances[$logkey] = new self($logkey);
        return self::$instances[$logkey];
    }

    public function add($row) {
        $result = LogQueue::instance('client')->usePut($this->_logkey, json_encode($row));
        if(!$result) 
            file_put_contents($this->_queuefile, date("Y-m-d H:i:s")."\t{$this->_logkey}\t".json_encode($row), FILE_APPEND);
        return $result;
    }

    public function setLevel($level = Logger::DEBUG) {
        $this->_level = $level;
        $this->resetLogger();
        return $this;
    }

    public function useFile($flag = false) {
        $this->_useFile = $flag ? true : false;
        $this->resetLogger();
        return $this;
    }

    public function useStdout($flag = false) {
        $this->_useStdout = $flag ? true : false;
        $this->resetLogger();
        return $this;
    }

    public function useEs($flag = true) {
        $this->_useEs = $flag ? true : false;
        $this->resetLogger();
        return $this;
    }

    public function debug($msg) {
        $this->logger()->debug($msg);
    }

    public function info($msg) {
        $this->logger()->info($msg);
    }

    public function notice($msg) {
        $this->logger()->notice($msg);
    }

    public function warn($msg) {
        $this->logger()->warn($msg);
    }

    public function error($msg) {
        $this->logger()->error($msg);
    }

    public function resetLogger() {
        unset(self::$loggers[$this->_logkey]);
        $this->logger();
    }

    public function logger() {
        if(!isset(self::$loggers[$this->_logkey])) {
            $logger = new Logger($this->_logkey);

            if($this->_useEs)     self::setLoggerEs    ($logger, $this->_logkey, $this->_level);
            if($this->_useStdout) self::setLoggerStdout($logger, $this->_logkey, $this->_level);
            if($this->_useFile)   self::setLoggerFile  ($logger, $this->_logkey, $this->_level);

            self::$loggers[$this->_logkey] = $logger;
        }
        return self::$loggers[$this->_logkey];
    }

    static public function setLogger($logkey, $logger, $level = Logger::DEBUG) {
        $esHandler = new EsHandler($logkey, $level);
        $esHandler->setFormatter(new LineFormatter("[%datetime%] %channel%.%level_name%: %message% %context% %extra% \n", '', true));
        $logger->pushHandler($esHandler);
        self::$loggers[$logkey] = $logger;
    }

    static public function setLoggerStdout($logger, $logkey, $level = Logger::DEBUG) {
        $logfile = 'php://stdout';
        $streamHandler = new StreamHandler($logfile, $level);
        $streamHandler->setFormatter(new LineFormatter("[%datetime%] %channel%.%level_name%: %message% %context% %extra% \n", '', true));
        $logger->pushHandler($streamHandler);
    }

    static public function setLoggerFile($logger, $logkey, $level = Logger::DEBUG) {
        $logdir = Cfg::instance()->get('logdir');
        $logpre = Cfg::instance()->get('logpre');
        $logfile = "{$logdir}/{$logpre}{$logkey}.log";
        $streamHandler = new StreamHandler($logfile, $level);
        $streamHandler->setFormatter(new LineFormatter("[%datetime%] %channel%.%level_name%: %message% %context% %extra% \n", '', true));
        $logger->pushHandler($streamHandler);
    }

    static public function setLoggerEs($logger, $logkey, $level = Logger::DEBUG) {
        $esHandler = new EsHandler($logkey, $level);
        $esHandler->setFormatter(new LineFormatter("[%datetime%] %channel%.%level_name%: %message% %context% %extra% \n", '', true));
        $logger->pushHandler($esHandler);
    }

}

