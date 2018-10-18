<?php
namespace Jingwu\LogEs;

class Flume extends Core {

    static $_instances = [];
    private $_cfg = null;
    public function __construct() {
        $this->_cfg = Cfg::instance()->get('flume');
        //初始化CURL
        Curl::instance('flume')->setZip(1);
    }

    static public function instance($key = 'default') {
        if(!isset(self::$_instances[$key])) self::$_instances[$key] = new self();
        return self::$_instances[$key];
    }

    public function post($data = []) {
        $json = is_string($data) ? $data : json_encode($data, 1);
        $result = $ids  = [];
        $j = 0;
        for($i = 0; $i < 3; $i++) {
            while($j++ < 50) {
                $id = rand(0, count($this->_cfg) - 1);
                if(in_array($id, $ids)) continue;
                break;
            }
            $result = Curl::instance('flume')->post($this->_cfg[$id], $json);
            if(isset($result['code']) && $result['code'] == 200) break;
            $ids[] = $id;
        }
        return $result;
    }

    public function listen($prefix) {
        $prefix = substr($prefix, 0, 4) == "log_" ? $prefix : "log_".$prefix;
        $prefixLen = strlen($prefix);

        static $logs = [];
        $reconn = 0;
        $limitTotal = 5000;
        $log = LogQueue::instance();
        $i = 0;
        while(1) {
            //连接异常，重连
            if($reconn == 1) { sleep(1); $log->reconnect(); $reconn = 0; }
            print_r("flume----".$i++."\n");

            $rows = $log->listTubes();
            if($rows === false) { $reconn = 1; continue; }
            $tubes = array_filter($rows, function($v) use($prefix) {return strpos($v, $prefix) === 0 ? true : false;});
            if(!$tubes) { sleep(1); continue; }

            $total = 0;
            foreach($tubes as $tube) {
                if($reconn == 1) break;
                if($total > $limitTotal) { break; }

                $stats = $log->statsTube($tube);
                if($stats === false) { $reconn = 1; break; }
                if(!$stats['current-jobs-ready']) continue;
                $result = $log->watch($tube);
                if($result === false) { $reconn = 1; break; }
                $tubesWatched = $log->listTubesWatched();
                if($tubesWatched === false) { $reconn = 1; break; }
                foreach($tubesWatched as $tubeIgnore => $row) {
                    if($tubeIgnore != $tube) $log->ignore($tubeIgnore);
                }
                $count = 0;
                while($count < $stats['current-jobs-ready']) {
                    if($total > $limitTotal) { break; }
                    $job    = $log->reserve(1);
                    if($job === false) { $reconn = 1; break; }
                    $logs[] = ["headers" => ["topic" => $tube], "body" => $job["body"]];
                    $result = $log->delete($job['id']);
                    if($result === false) { $reconn = 1; break; }
                    $count++;
                    $total++;
                }
            }
            Flume::instance()->post($logs);
            $logs = [];
        }

    }

}

