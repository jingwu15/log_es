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
            if($rows === false) { print_r("连接失败\n"); $reconn = 1; continue; }
            $tubes = array_filter($rows, function($v) use($prefix) {return strpos($v, $prefix) === 0 ? true : false;});
            if(!$tubes) { sleep(1); continue; }

            $total = 0;
            foreach($tubes as $tube) {
                //print_r("\ntube-{$tube}: start\n");
                if($reconn == 1) break;
                if($total > $limitTotal) { break; }
                $tubeMap = $tubeKeys = [];
                if(!isset($tubeMap[$tube])) {
                    $doc = $tube."_".date('Y_m');
                    $result = EsClient::instance($doc)->getMap();
                    //没有取到文档结构，有可能是其他业务的日志，不做处理
                    if(!$result['code']) continue;
                    $tubeMap = $result['data'][$tube]["properties"];
                    $tubeKeys = array_keys($tubeMap);
                }

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
                //print_r("tube-{$tube}-count: {$count}\n");
                while($count < $stats['current-jobs-ready']) {
                    if($total > $limitTotal) { break; }
                    $job    = $log->reserve(1);
                    if($job === false) { $reconn = 1; break; }
                    $jdata = json_decode($job["body"], 1);
                    if(!$jdata) {
                        //错误，非json格式
                        file_put_contents("/tmp/log_queue_error.log", date('Y-m-d H:i:s')."\t{$tube}\t{$job['body']}\n", FILE_APPEND);
                        $result = $log->delete($job['id']);
                        continue;
                    }
                    $keys = array_keys($jdata);
                    $diff = array_diff($keys, $tubeKeys);
                    //print_r("tube-{$tube}-diff: ".json_encode($diff, JSON_UNESCAPED_UNICODE)."\n");
                    if($diff) {
                        //有新增字段
                        file_put_contents("/tmp/log_queue.log", date('Y-m-d H:i:s')."\t{$tube}\t{$job['body']}\n", FILE_APPEND);
                        $result = $log->delete($job['id']);
                        continue;
                    }
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

