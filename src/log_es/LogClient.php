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
    protected $_logfile      = '';
    protected $_level        = Logger::DEBUG;
    protected $_useEs        = true;
    protected $_useFile      = false;
    protected $_useStdout    = false;
    protected $_queueFile    = '';
    static public $loggers   = [];
    static public $instances = [];

    public function __construct($logkey) {
        $this->_logkey = $logkey;
        $logdir = Cfg::instance()->get('logdir');
        $this->_queueFile = "{$logdir}/log_queue.log";
        $this->_logfile = "{$logdir}/{$logkey}.log";
    }

    static public function instance($logkey = 'default') {
        $logkey = trim($logkey);
        if(empty($logkey)) throw new Exception('logkey is empty');

        if(!isset(self::$instances[$logkey])) self::$instances[$logkey] = new self($logkey);
        return self::$instances[$logkey];
    }

    public function resetLogger() {
        unset(self::$loggers[$this->_logkey]);
        $this->logger();
    }

    public function logger() {
        if(!isset(self::$loggers[$this->_logkey])) {
            $logger = new Logger($this->_logkey);

            if($this->_useEs)     self::setLoggerEs    ($logger, $this->_logkey, $this->_level);
            if($this->_useStdout) self::setLoggerStdout($logger, $this->_level);
            if($this->_useFile)   self::setLoggerFile  ($logger, $this->_logfile, $this->_level);

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

    static public function setLoggerStdout($logger, $level = Logger::DEBUG) {
        $logfile = 'php://stdout';
        $streamHandler = new StreamHandler($logfile, $level);
        $streamHandler->setFormatter(new LineFormatter("[%datetime%] %channel%.%level_name%: %message% %context% %extra% \n", '', true));
        $logger->pushHandler($streamHandler);
    }

    static public function setLoggerFile($logger, $logfile, $level = Logger::DEBUG) {
        $streamHandler = new StreamHandler($logfile, $level);
        $streamHandler->setFormatter(new LineFormatter("[%datetime%] %channel%.%level_name%: %message% %context% %extra% \n", '', true));
        $logger->pushHandler($streamHandler);
    }

    static public function setLoggerEs($logger, $logkey, $level = Logger::DEBUG) {
        $esHandler = new EsHandler($logkey, $level);
        $esHandler->setFormatter(new LineFormatter("[%datetime%] %channel%.%level_name%: %message% %context% %extra% \n", '', true));
        $logger->pushHandler($esHandler);
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

    public function add($row) {
        $now = date("Y-m-d H:i:s");
        if($this->_useEs) {
            $body = json_encode($row, JSON_UNESCAPED_UNICODE);
            $result = LogQueue::instance('client')->usePut($this->_logkey, $body);
            if(!$result) file_put_contents($this->_queueFile, "{$now}\t{$this->_logkey}\t{$body}\n", FILE_APPEND);
        }
        if($this->_useFile) {
            $body = json_encode($row, JSON_UNESCAPED_UNICODE);
            file_put_contents($this->_logfile, "{$now}\t{$this->_logkey}\t{$body}\n", FILE_APPEND);
        }
        if($this->_useStdout) {
            $body = json_encode($row, JSON_UNESCAPED_UNICODE | JSON_PRETTY_PRINT);
            print_r("{$now}\t{$this->_logkey}\t{$body}\n");
        }
        return $result;
    }

    public function emergency($message, array $context = array()) {
        $this->logger()->emergency($message, $context);
    }
    public function emerg($message, array $context = array()) {
        $this->logger()->emerg($message, $context);
    }
    public function alert($message, array $context = array()) {
        $this->logger()->alert($message, $context);
    }
    public function critical($message, array $context = array()) {
        $this->logger()->critical($message, $context);
    }
    public function crit($message, array $context = array()) {
        $this->logger()->crit($message, $context);
    }
    public function error($message, array $context = array()) {
        $this->logger()->error($message, $context);
    }
    public function err($message, array $context = array()) {
        $this->logger()->err($message, $context);
    }
    public function warning($message, array $context = array()) {
        $this->logger()->warning($message, $context);
    }
    public function warn($message, array $context = array()) {
        $this->logger()->warn($message, $context);
    }
    public function notice($message, array $context = array()) {
        $this->logger()->notice($message, $context);
    }
    public function info($message, array $context = array()) {
        $this->logger()->info($message, $context);
    }
    public function debug($message, array $context = array()) {
        $this->logger()->debug($message, $context);
    }

}

