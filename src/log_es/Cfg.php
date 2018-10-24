<?php
namespace Jingwu\LogEs;

class Cfg extends Core {

    private $_cfg = null;
    static public $instances = [];

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
            'es' => [],
            'tmpdir' => '/tmp',
            'logpre' => 'log_',
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
        if(!$this->_formatStrArr($apis)) return false;
        $this->_cfg['es'] = $apis;
    }

    public function setMails($mails) {
        if(!$this->_formatStrArr($mails)) return false;
        $this->_cfg['mails'] = $mails;
    }

    public function setTmpdir($tmpdir) {
        $this->_cfg['tmpdir'] = $tmpdir;
        $this->_cfg['tmpdir'] = $this->_formatPath($this->_cfg['tmpdir']);
    }

    public function setLogpre($logpre) {
        $this->_cfg['logpre'] = $logpre ? $logpre : $this->_cfg['logpre'];
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
        if(!$this->_cfg['flume']) return ['code' => 0, "error" => "flume noset"    ];
        if(!$this->_cfg['es']) return ['code' => 0, "error" => "es noset"       ];
        if(!$this->_cfg['mails']) return ['code' => 0, "error" => "mail noset"     ];

        $cfgBs = $this->_cfg['beanstalk'];
        if(!$cfgBs['host'] || !$cfgBs['port']) return ['code' => 0, "error" => "beanstalk set error"];
        return ['code' => 1, "error" => "ok"];
    }

}

