<?php
namespace Jingwu\LogEs;

class Cfg extends Core {

    private $_cfg = null;
    static public $instances = [];
    
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
        $this->_cfg['flume'] = $apis;
    }

    public function setEs($hosts) {
        $this->_cfg['es'] = $hosts;
    }

    public function setLog($key, $fields) {
        $this->_cfg['logs'][$key] = $fields;
    }

    public function setLogs($docs) {
        $this->_cfg['logs'] = $docs;
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

}

