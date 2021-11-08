<?php
namespace Jingwu\LogEs;

class Cfg extends Core {

    private $_cfg = null;
    static public $instances = [];
    static public $bodySizeMin = 64 * 1024;
    static public $bodySizeMax = 1024 * 1024;

    public function __construct() {
        //初始化
        $this->_cfg = [
            'beanstalk' => [
                'host'           => '',
                'port'           => '',
                'persistent'     => true,
                'timeout'        => 600,
                'socket_timeout' => 3600
            ],
            'flume' => [],
            'logdir' => '/tmp',
            'logpre' => 'log_',
            'limit' => [
                'limit_write' => 50000,
                'body_size_max' => self::$bodySizeMin,
            ],
            'mail'  => [
                'mails' => [],
                'interval' => 300,
            ],
        ];
    }

    static public function instance($key = 'default') {
        if(!isset(self::$instances[$key])) {
            self::$instances[$key] = new self();
        }
        return self::$instances[$key];
    }

    public function setBeanStalk($host, $port) {
        $this->_cfg['beanstalk'] = [
            'host'           => $host,
            'port'           => $port,
            'persistent'     => true,
            'timeout'        => 600,
            'socket_timeout' => 3600
        ];
    }

    public function setFlume($apis) {
        if(!$this->_formatStrArr($apis)) return false;
        $this->_cfg['flume'] = $apis;
    }

    public function setEs($apis) {
    }

    public function setMails($mails) {
        if(!$this->_formatStrArr($mails)) return false;
        $this->_cfg['mail']['mails'] = $mails;
    }

    public function setMailInterval($interval) {
        $interval = intval($interval);
        $interval = $interval > 60 ? $interval : 60;
        $this->_cfg['mail']['interval'] = $interval;
    }

    public function setLogdir($logdir) {
        $this->_cfg['logdir'] = $logdir;
        $this->_cfg['logdir'] = $this->_formatPath($this->_cfg['logdir']);
    }

    public function setLogpre($logpre) {
        $this->_cfg['logpre'] = $logpre ? $logpre : $this->_cfg['logpre'];
    }

    public function setMqEsdoc($mqEsdoc) {
    }

    public function setLimitWrite($limitWrite = 50000) {
        $this->_cfg['limit']['limit_write'] = $limitWrite ? $limitWrite : $this->_cfg['limit']['limit_write'];
    }

    public function setBodySizeMax($bodySizeMax = 0) {
        $this->_cfg['limit']['body_size_max'] = $bodySizeMax <= self::$bodySizeMin ? self::$bodySizeMax : $bodySizeMax;
    }

    public function get($key) {
        $keys = explode(".", $key);
        $tmp = $this->_cfg;
        foreach($keys as $field) {
            if(!isset($tmp[$field])) return false;
            $tmp = $tmp[$field];
        }
        return $tmp;
    }

    private function _formatPath($path) {
        return substr($path, -1) == '/' ? substr($path, 0, -1) : $path;
    }

    private function _formatStrArr($rows) {
        if(!is_array($rows)) return false;
        foreach($rows as $row) if(!is_string($row)) return false;
        return $rows;
    }

    public function check() {
        if(!$this->_cfg['beanstalk']) return ['code' => 0, "error" => "beanstalk noset"];
        if(!$this->_cfg['flume'])     return ['code' => 0, "error" => "flume noset"    ];
        if(!$this->_cfg['mail'])     return ['code' => 0, "error" => "mail noset"     ];

        $cfgBs = $this->_cfg['beanstalk'];
        if(!$cfgBs['host'] || !$cfgBs['port']) return ['code' => 0, "error" => "beanstalk set error"];
        return ['code' => 1, "error" => "ok"];
    }

}

