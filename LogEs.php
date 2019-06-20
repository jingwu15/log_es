<?php

namespace Service\LogEs;

use \Jingwu\LogEs\Cfg;
use \Jingwu\LogEs\Flume;
use \Jingwu\LogEs\LogQueue;
use Psr\Log\LoggerInterface;
use \Jingwu\LogEs\LogClient;
use Ypf\Lib\Config as YpfCfg;

class LogEs implements LoggerInterface {

    const DEBUG    = 100;        //Logger::DEBUG        LogClient::DEBUG
    const INFO     = 200;        //Logger::INFO         LogClient::INFO
    const NOTICE   = 250;        //Logger::NOTICE       LogClient::NOTICE
    const WARNING  = 300;        //Logger::WARNING      LogClient::WARNING
    const ERROR    = 400;        //Logger::ERROR        LogClient::ERROR
    const CRITICAL = 500;        //Logger::CRITICAL     LogClient::CRITICAL

    private $_logkey = 'default';
    private $_syskey = '';
    static public $instances = [];
    static public $initCfg = false;

    public function __construct($logkey = 'default') {
        if(defined('SYS_KEY')) $this->_syskey = SYS_KEY;
        $this->_logkey = defined('SYS_KEY') ? SYS_KEY.'-'.$logkey : $logkey;
    }

    static public function ins($logkey = 'default') {
        self::config();
        if(!isset(self::$instances[$logkey])) self::$instances[$logkey] = new self($logkey);
        return self::$instances[$logkey];
    }

    static public function config() {
        if(!self::$initCfg) {
            $flumeApis = $esApis = $mails = [];
            $cfg = YpfCfg::getInstance()->get('loges');
            $cfgBs = $cfg['beanstalk'];
            $bsHost = isset($cfgBs['host']) ? trim($cfgBs['host']) : null;
            $bsPort = isset($cfgBs['port']) ? trim($cfgBs['port']) : null;
            $logdir = isset($cfg['base']) && isset($cfg['base']['logdir']) ? trim($cfg['base']['logdir']) : '';
            $logpre = isset($cfg['base']) && isset($cfg['base']['logpre']) ? trim($cfg['base']['logpre']) : '';
            $mailInterval = isset($cfg['mail']) && isset($cfg['mail']['interval']) ? intval($cfg['mail']['interval']) : 0;
            if(isset($cfg['flume'])) {
                foreach($cfg['flume'] as $key => $value) if(substr($key, 0, 3) == 'api') $flumeApis[] = $value;
            }
            if(isset($cfg['es'])) {
                foreach($cfg['es'] as $key => $value) if(substr($key, 0, 3) == 'api') $esApis[] = $value;
            }
            if(isset($cfg['mail']) && isset($cfg['mail']['mails'])) {
                $mails = explode(",", $cfg['mail']['mails']);
                foreach($mails as &$mail) $mail = trim($mail);
            }
            $mqEsdoc = [];
            $cfgEsdocMqMap = isset($cfg['esdoc_mq_map']) ? $cfg['esdoc_mq_map'] : [];
            foreach($cfgEsdocMqMap as $esdoc => $line) {
                if(!$line) continue;
                $rows = explode(',', $line);
                foreach($rows as $mq) {
                    $mqEsdoc[$logpre.trim($mq)] = $esdoc;
                }
            }
            $limitWrite = isset($cfg['limit']) && isset($cfg['limit']['limit_write']) ? intval($cfg['limit']['limit_write']) : 5000;

            if($bsHost && $bsPort) Cfg::instance()->setBeanstalk($bsHost, $bsPort);
            if($esApis)       Cfg::instance()->setEs($esApis);
            if($mails)        Cfg::instance()->setMails($mails);
            if($logdir)       Cfg::instance()->setLogdir($logdir);
            if($logpre)       Cfg::instance()->setLogpre($logpre);
            if($flumeApis)    Cfg::instance()->setFlume($flumeApis);
            if($mailInterval) Cfg::instance()->setMailInterval($mailInterval);
            if($limitWrite)   Cfg::instance()->setLimitWrite($limitWrite);
            if($mqEsdoc)      Cfg::instance()->setMqEsdoc($mqEsdoc);
            self::$initCfg = true;
        }
        return true;
    }

    static public function toFlume($prefix = '') {
        self::config();
        Flume::instance()->listen($prefix);
    }

    static public function correct($prefix = '') {
        self::config();
        Flume::instance()->correct($prefix);
    }

    public function setLevel($level = self::DEBUG) {
        LogClient::instance($this->_logkey)->setLevel($level);
        return $this;
    }
    public function useStdout($flag = false) {
        LogClient::instance($this->_logkey)->useStdout($flag);
        return $this;
    }
    public function useFile($flag = false) {
        LogClient::instance($this->_logkey)->useFile($flag);
        return $this;
    }
    public function useEs($flag = true) {
        LogClient::instance($this->_logkey)->useEs($flag);
        return $this;
    }

    public function add($data) {
        return LogClient::instance($this->_logkey)->add($data);
    }
    public function emergency($message, array $context = array()) {
    }
    public function emerg($message, array $context = array()) {
    }
    public function alert($message, array $context = array()) {
    }
    public function critical($message, array $context = array()) {
    }
    public function error($message, array $context = array()) {
        LogClient::instance($this->_logkey)->error($message);
    }
    public function warning($message, array $context = array()) {
        LogClient::instance($this->_logkey)->warn($message);
    }
    public function warn($message, array $context = array()) {
        LogClient::instance($this->_logkey)->warn($message);
    }
    public function notice($message, array $context = array()) {
        LogClient::instance($this->_logkey)->notice($message);
    }
    public function info($message, array $context = array()) {
        LogClient::instance($this->_logkey)->info($message);
    }
    public function debug($message, array $context = array()) {
        LogClient::instance($this->_logkey)->debug($message);
    }
    public function log($level, $message, array $context = array()) {
    }

}

