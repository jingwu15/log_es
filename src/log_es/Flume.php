<?php
namespace Jingwu\LogEs;

class Flume extends Core {

    private $_apis = [];
    static public $instances = [];

    public function __construct() {
        //初始化CURL
        Curl::instance('flume')->setZip(1);
        $this->_apis = Cfg::instance()->get('flume');
    }

    static public function instance($key = 'default') {
        if(!isset(self::$instances[$key])) self::$instances[$key] = new self();
        return self::$instances[$key];
    }

    public function post($data = []) {
        $json = is_string($data) ? $data : json_encode($data, JSON_UNESCAPED_UNICODE);
        $result = $ids  = [];
        $j = 0;
        for($i = 0; $i < 3; $i++) {
            while($j++ < 50) {
                $id = rand(0, count($this->_apis) - 1);
                if(in_array($id, $ids)) continue;
                break;
            }
            $result = Curl::instance('flume')->post($this->_apis[$id], $json);
            if(isset($result['code']) && $result['code'] == 200) break;
            $ids[] = $id;
        }
        return $result;
    }

    public function _formatLogFile($prefix) {
        $prefix     = in_array(substr($prefix, -1), ['_', '-']) ? substr($prefix, 0, -1) : $prefix;
        $logdir     = Cfg::instance()->get('logdir');
        $logLock    = "{$logdir}/logauto_{$prefix}.lock";
        $logError   = "{$logdir}/logauto_{$prefix}.error.log";
        $logFile    = "{$logdir}/logauto_{$prefix}.log";
        $logTmp     = "{$logdir}/logauto_{$prefix}.log.tmp";
        $logCorrect = "{$logdir}/logauto_{$prefix}.log.correct";
        return [$logLock, $logError, $logFile, $logTmp, $logCorrect];
    }

    public function listen($prefix = '') {
        $logpre = trim(Cfg::instance()->get('logpre'));
        $prefix = $prefix ? trim($prefix) : $logpre;
        if(!$prefix) throw new \Exception("the log prefix is not allow empty!");
        list($logLock, $logError, $logFile, $logTmp, $logCorrect) = $this->_formatLogFile($prefix);

        $limitMail = Cfg::instance()->get('mail.interval');   //间隔5分钟
        $mqDocMap = Cfg::instance()->get('mq_esdoc');   //mq 与 es文档的映射
        $limitWrite = Cfg::instance()->get('limit.limit_write');   //写入限制，每次读取多少条数据写入ES
        $reconn = 0;
        $mailsNoDoc = $mailsDiff = [];
        $log = LogQueue::instance();
        while(1) {
            sleep(1);
            //连接异常，重连
            if($reconn == 1) { sleep(1); $log->reconnect(); $reconn = 0; }
            print_r(sprintf("flume-listen---%s\n", date("Y-m-d H:i:s")));

            $rows = $log->listTubes();
            if($rows === false) { print_r("连接失败\n"); $reconn = 1; sleep(1); continue; }
            $tubes = array_filter($rows, function($v) use($prefix) {return strpos($v, $prefix) === 0 ? true : false;});
            if(!$tubes) { sleep(1); continue; }

            $logs = $logsCorrect = $logsError = $ids = $idsCorrect = $idsError = [];
            $total = 0;
            foreach($tubes as $tube) {
                //print_r("\ntube-{$tube}: start\n");
                if($reconn == 1) break;
                if($total > $limitWrite) { break; }
                $esdoc = isset($mqDocMap[$tube]) ? $mqDocMap[$tube] : $tube;
                $tubeMap = $tubeKeys = [];
                if(!isset($tubeMap[$tube])) {
                    $doc = substr($esdoc, strlen($logpre));
                    $docYm = sprintf("%s_%s", $doc, date('Y_m'));
                    $result = EsClient::instance($docYm)->getMap();
                    //没有取到文档结构，有可能是其他业务的日志，不做处理
                    if(!$result['code']) {
                        if(!isset($mailsNoDoc[$tube])) $mailsNoDoc[$tube] = 0;
                        continue;
                    }
                    $tubeMap = $result['data'][$esdoc]["properties"];
                    $tubeKeys = array_keys($tubeMap);
                }

                $stats = $log->statsTube($tube);
                if($stats === false) { $reconn = 1; break; }
                if(!$stats['current-jobs-ready']) continue;
                $count = 0;
                $log->reconnect();
                $result = $log->watch($tube);
                //print_r("tube-{$tube}-count: {$count}\n");
                while($count < $stats['current-jobs-ready']) {
                    if($total > $limitWrite) { break; }
                    $job    = $log->reserve(1);
                    if($job === false) { $reconn = 1; break; }
                    $jdata = json_decode($job["body"], 1);
                    if(!$jdata) {         //错误，非json格式, 直接丢弃
                        $idsError[] = $job['id'];
                        $logsError[] = sprintf("%s\t%s\t%s\n", date('Y-m-d H:i:s'), $tube, $job['body']);
                        continue;
                    }
                    $keys = array_keys($jdata);
                    $diff = array_diff($keys, $tubeKeys);
                    //print_r("tube-{$tube}-diff: ".json_encode($diff, JSON_UNESCAPED_UNICODE)."\n");
                    if($diff) {        //有新增字段, 需要校正ES中的文档结构
                        $idsCorrect[]  = $job['id'];
                        $logsCorrect[] = sprintf("%s\t%s\t%s", date('Y-m-d H:i:s'), $tube, json_encode($jdata, JSON_UNESCAPED_UNICODE));

                        $body = implode(", ", $diff);
                        $diffKey = md5($tube.$body);
                        if(isset($mailsDiff[$diffKey])) continue;
                        $mailsDiff[$diffKey] = ['ctime' => 0, 'logkey' => $tube, 'body' => $body];
                    }
                    $ids[] = $job['id'];
                    $logs[] = ["headers" => ["topic" => $esdoc], "body" => $job["body"]];
                    $count++;
                    $total++;
                }
            }
            $this->post($logs);
            $ids = array_merge($ids, $idsCorrect, $idsError);
            foreach($ids as $id) $log->delete($id);
            //邮件
            foreach($mailsNoDoc as $logkey => &$ctime) {
                if(!$ctime) { $this->mailNoDoc($logkey); $ctime = time(); }
                if((time() - $ctime) > $limitMail) unset($mailsNoDoc[$logkey]);
            }
            foreach($mailsDiff as $diffKey => &$row) {
                if(!$row['ctime']) { $this->mailLackField($row['logkey'], $row['body']); $row['ctime'] = time(); }
                if((time() - $row['ctime']) > $limitMail) unset($mailsDiff[$diffKey]);
            }
            $fp = fopen($logLock, 'a+');
            flock($fp, LOCK_EX);
            if($logsError) self::lockAppend($logError, implode("\n", $logsError)."\n");
            if($logsCorrect) self::lockAppend($logFile, implode("\n", $logsCorrect)."\n");
            flock($fp, LOCK_UN);
            fclose($fp);
        }
    }

    //校正少字段的文档
    public function correct($prefix = '') {
        $logpre = trim(Cfg::instance()->get('logpre'));
        $prefix = $prefix ? trim($prefix) : $logpre;
        if(!$prefix) throw new \Exception("the log prefix is not allow empty!");
        list($logLock, $logError, $logFile, $logTmp, $logCorrect) = $this->_formatLogFile($prefix);
        $mqDocMap = Cfg::instance()->get('mq_esdoc');   //mq 与 es文档的映射

        //初始化时，校验是否未完成，重新修正
        if(file_exists($logTmp)) {
            //锁定配置文件，本次校正结束
            $fp = fopen($logLock, 'w+');
            flock($fp, LOCK_EX);

            //合并临时文件及新校正文件
            if(file_exists($logFile)) {
                $content = trim(file_get_contents($logFile));
                file_put_contents($logTmp, $content."\n", FILE_APPEND);
            }
            rename($logTmp, $logFile);

            if(file_exists($logTmp)) unlink($logTmp);
            if(file_exists($logCorrect)) unlink($logCorrect);
            flock($fp, LOCK_UN);
            fclose($fp);
        }

        $limitMail  = Cfg::instance()->get('mail.interval');   //间隔5分钟
        $limitWrite = Cfg::instance()->get('limit.limit_write');   //写入限制，每次读取多少条数据写入ES
        $mailsDiff = [];
        while(1) {
            //5秒执行一次
            sleep(5);
            print_r(sprintf("flume-correct---%s\n", date("Y-m-d H:i:s")));

            if(!file_exists($logFile)) continue;
            //锁定配置文件，不再读写错误日志文件，并重命名，以进行后面的数据校正
            $fp = fopen($logLock, 'w+');
            flock($fp, LOCK_EX);
            rename($logFile, $logTmp);
            flock($fp, LOCK_UN);
            fclose($fp);

            $logs = $logkeys = $logsCorrect = $logsError = [];
            $count = $countCorrect = $countError = $exit = 0;
            $fp = fopen($logTmp, 'r');
            while(1) {
                if($count > $limitWrite || $exit) {
                    //写入Flume, 并重新计数
                    $this->post($logs);
                    $logs = [];
                    $count = 0;
                }
                if($countCorrect > $limitWrite || $exit) {
                    $content = implode("\n", $logsCorrect);
                    if($content) self::lockAppend($logCorrect, $content."\n");
                    $logsCorrect = [];
                    $countCorrect = 0;
                }
                if($countError > $limitWrite || $exit) {
                    $content = implode("\n", $logsError);
                    if($content) self::lockAppend($logError, $content."\n");
                    $logsError = [];
                    $countError = 0;
                }
                if($exit) break;
                if(feof($fp)) { $exit = 1; continue; }
                $line = trim(fgets($fp));
                if(!$line) continue;
                $lArr = explode("\t", $line, 3);
                if(count($lArr) < 3) { $logsError[] = $line; $countError++; continue; }
                $esdoc = isset($mqDocMap[$logkey]) ? $mqDocMap[$logkey] : $logkey;
                $logkey = $lArr[1];
                if(!isset($logkeys[$logkey])) {
                    $doc = substr($esdoc, strlen($logpre));
                    $docYm = sprintf("%s_%s", $doc, date('Y_m'));
                    $result = EsClient::instance($doc)->getMap();
                    $resultYm = EsClient::instance($docYm)->getMap();
                    //没有取到文档结构，有可能是其他业务的日志，不做处理
                    if($result['code']) {
                        //var_dump(date("Y-m-d H:i:s")."\t{$doc}\t".json_encode($result, JSON_UNESCAPED_UNICODE));
                        $logkeys[$logkey] = array_keys($result['data'][$esdoc]["properties"]);
                    }
                    if($resultYm['code']) {
                        //var_dump(date("Y-m-d H:i:s")."\t{$docYm}\t".json_encode($resultYm, JSON_UNESCAPED_UNICODE));
                        $logkeys[$logkey] = array_keys($resultYm['data'][$esdoc]["properties"]);
                    }
                    $logkeys[$logkey] = isset($logkeys[$logkey]) ? $logkeys[$logkey] : [];
                }
                $jdata = json_decode($lArr[2], 1);
                //不能正确的解析json, 则格式不正确，丢弃
                if(!$jdata) { $logsError[] = $line; $countError++; continue; }
                $diff = array_diff(array_keys($jdata), $logkeys[$logkey]);
                if($logkeys[$logkey] && $diff) {
                    $logsCorrect[] = $line;
                    $countCorrect++;

                    $body = implode(", ", $diff);
                    $diffKey = md5($logkey.$body);
                    if(isset($mailsDiff[$diffKey])) continue;
                    $mailsDiff[$diffKey] = ['ctime' => 0, 'logkey' => $logkey, 'body' => $body];
                } else {
                    $logs[] = ["headers" => ["topic" => $esdoc], "body" => $lArr[2]];
                    $count++;
                }
            }

            //发邮件
            foreach($mailsDiff as $diffKey => &$row) {
                if(!$row['ctime']) { $this->mailLackField($row['logkey'], $row['body']); $row['ctime'] = time(); }
                if((time() - $row['ctime']) > $limitMail) unset($mailsDiff[$diffKey]);
            }

            //锁定配置文件，本次校正结束
            $fp = fopen($logLock, 'w+');
            flock($fp, LOCK_EX);

            //将新内容追加到过滤后校正的文件中
            if(file_exists($logFile)) {
                $content = trim(file_get_contents($logFile));
                file_put_contents($logCorrect, $content."\n", FILE_APPEND);
            }

            //将过滤后校正的文件 重命名为校正文件
            rename($logCorrect, $logFile);

            //删除过滤用的临时文件
            unlink($logTmp);

            flock($fp, LOCK_UN);
            fclose($fp);
        }
    }

    static public function lockAppend($fname, $data) {
        $fp = fopen($fname, 'a+');
        if(!$fp) return false;
        flock($fp, LOCK_EX);
        fwrite($fp, $data);
        flock($fp, LOCK_UN);
        fclose($fp);
    }

    static public function lockWrite($fname, $data) {
        $fp = fopen($fname, 'w+');
        if(!$fp) return false;
        flock($fp, LOCK_EX);
        file_put_contents($fname, $data);
        flock($fp, LOCK_UN);
        fclose($fp);
    }

    static public function lockRead($fname) {
        $fp = fopen($fname, 'r+');
        if(!$fp) return false;
        flock($fp, LOCK_EX);
        $data = file_get_contents($fname);
        flock($fp, LOCK_UN);
        fclose($fp);
        return $data;
    }

    public function mailLackField($logkey, $fieldStr) {
        $body = "日志[{$logkey}]结构有变更，缺少字段: {$fieldStr}，请及时更新ES结构! ";
        $this->sendmail($logkey, $body);
    }

    public function mailNoDoc($logkey) {
        $body = "ES中，文档{$logkey}不存在，请及时更新ES结构! ";
        $this->sendmail($logkey, $body);
    }

    public function sendmail($logkey, $body) {
        $mails = Cfg::instance()->get('mail.mails');
        $subject = "日志结构变更[{$logkey}]";
$html = <<<EOF
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml">
<head>
    <meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
</head>
<body>
<style type="text/css">
.qmbox{font-family: "微软雅黑";	font-size: 14px;}
.qmbox table{border: 1px solid #ccc; border-bottom: 0px;	border-right: 0px;}
.qmbox table td{border-bottom: 1px solid #ccc; border-right: 1px solid #ccc;}
.b{font-weight:bold;}
.f{float:left;}
.ydtd{width:130px; text-align:center;}
</style>
<div class="qmbox">
	<div>
		<p>{$body}</p>
	</div>
</div>
</body>
</html>
EOF;
        $ids = [];
        foreach($mails as $mail) {
            $content = json_encode([
                'mailto'     => $mail,
                'subject'    => $subject,
                'body'       => $html,
                'type'       => 'html',
                //'plat'       => 'plat',
                'email_type' => 'trigger',
            ], JSON_UNESCAPED_UNICODE);
            $ids[] = LogQueue::instance()->usePut('new_mailsend', $content);
        }
        return $ids;
    }

}

