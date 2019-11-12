<?php
namespace Jingwu\LogEs;

class Flume extends Core {

    private $_apis = [];
    static public $memLimit = 0;
    static public $memLimit80 = 0;
    static public $instances = [];
    static public $bodySizeMax = 1024 * 1024;

    public function __construct() {
        //初始化CURL
        Curl::instance('flume')->setZip(1);
        $this->_apis = Cfg::instance()->get('flume');

        $memLimitStr = ini_get('memory_limit');
        switch(substr($memLimitStr, -1)) {
        case 'K':
            self::$memLimit = substr($memLimitStr, 0, -1) * 1024; break;
        case 'K':
            self::$memLimit = substr($memLimitStr, 0, -1) * 1024; break;
        case 'm':
            self::$memLimit = substr($memLimitStr, 0, -1) * 1024 * 1024; break;
        case 'M':
            self::$memLimit = substr($memLimitStr, 0, -1) * 1024 * 1024; break;
        case 'g':
            self::$memLimit = substr($memLimitStr, 0, -1) * 1024 * 1024 * 1024; break;
        case 'G':
            self::$memLimit = substr($memLimitStr, 0, -1) * 1024 * 1024 * 1024; break;
        default:
            self::$memLimit = substr($memLimitStr, 0, -1) * 1024 * 1024 * 1024; break;
        }
        self::$memLimit80 = self::$memLimit * 0.8;
    }

    static public function instance($key = 'default') {
        if(!isset(self::$instances[$key])) self::$instances[$key] = new self();
        return self::$instances[$key];
    }

    public function post($data = []) {
        $json = is_string($data) ? $data : json_encode($data, JSON_UNESCAPED_UNICODE);
        $result = $ids  = $usedApis = [];
        $j = 0;
        for($i = 0; $i < 3; $i++) {
            while($j++ < 50) {
                $id = rand(0, count($this->_apis) - 1);
                if(in_array($id, $ids)) continue;
                break;
            }
            $usedApis[] = $this->_apis[$id];
            //Expect: 是为了处理continue问题
            $result = Curl::instance('flume')->post($this->_apis[$id], $json, ['Expect:']);
            if(isset($result['code']) && $result['code'] == 200) break;
            $ids[] = $id;
        }
        return array_merge($result, ['used_apis' => $usedApis]);
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
                    $esdocItems = [
                        sprintf("%s_%s", $esdoc, date('Y_m')),
                        sprintf("%s_%s", $esdoc, date('Y')),
                    ];
                    //没有取到文档结构，有可能是其他业务的日志，不做处理
                    $flagInEs = false;
                    foreach($esdocItems as $esdocItem) {
                        $result = EsClient::instance($esdocItem)->getMap();
                        if($result['code']) {
                            //取第一个，忽略有多个log_type的问题
                            $first = current($result['data']);
                            $tubeMap = $first["properties"];
                            $tubeKeys = array_keys($tubeMap);
                            $flagInEs = true;
                            break;
                        }
                    }
                    //文档不存在，不处理
                    if($flagInEs) {
                        unset($mailsNoDoc[$tube]);
                    } else {
                        if(!isset($mailsNoDoc[$tube])) $mailsNoDoc[$tube] = 0;
                        continue;
                    }
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
                    // 内存超过限制数量的80％，不再读取新任务
                    if(memory_get_usage() > self::$memLimit80) { break; }
                    $job    = $log->reserve(1);
                    if($job === false) { $reconn = 1; break; }
                    $jdata = json_decode($job["body"], 1);
                    if(!$jdata) {         //错误，非json格式, 直接丢弃
                        $idsError[] = $job['id'];
                        $logsError[] = sprintf("%s\t%s\t%s\n", date('Y-m-d H:i:s'), $tube, $job['body']);
                        continue;
                    }
                    if(strlen($job['body']) > self::$bodySizeMax) {        //消息体超长
                        $idsError[] = $job['id'];
                        //不再记录超长的消息
                        //$logsError[] = sprintf("%s\t%s\t%s\n", date('Y-m-d H:i:s'), $tube, $job['body']);
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

            //请求异常, 正常消息不做处理(不删除，即自动恢复到正常的状态)， 异常消息删除队列任务
            $resp = $this->post($logs);
            if($resp['code'] == 200) {
                $ids = array_merge($ids, $idsCorrect, $idsError);
            } else {
                $ids = array_merge($idsCorrect, $idsError);
                $this->mailReqEsFail(implode(', ', $resp['used_apis']));
            }
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
                $line = trim(fgets($fp, self::$bodySizeMax));
                if(!$line) continue;
                $lArr = explode("\t", $line, 3);
                if(count($lArr) < 3) { $logsError[] = $line; $countError++; continue; }
                $logkey = $lArr[1];
                $esdoc = isset($mqDocMap[$logkey]) ? $mqDocMap[$logkey] : $logkey;
                if(!isset($logkeys[$logkey])) {
                    $esdocItems = [
                        sprintf("%s_%s", $esdoc, date('Y_m')),
                        $esdoc,
                        sprintf("%s_%s", $esdoc, date('Y')),
                    ];
                    $logkeys[$logkey] = isset($logkeys[$logkey]) ? $logkeys[$logkey] : [];
                    foreach($esdocItems as $esdocItem) {
                        $result = EsClient::instance($esdocItem)->getMap();
                        if($result['code']) {
                            $first = current($result['data']);
                            $tubeMap = $first["properties"];
                            $logkeys[$logkey] = array_keys($tubeMap);
                            break;
                        }
                    }
                }
                $jdata = json_decode($lArr[2], 1);
                //不能正确的解析json, 则格式不正确，丢弃
                if(!$jdata) { $logsError[] = $line; $countError++; continue; }
                //消息超长
                if(strlen($lArr[2]) > self::$bodySizeMax) { $logsError[] = $line; $countError++; continue; }
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

    public function mailReqEsFail($apis) {
        $subject = "LogEs Flume接口异常";
        $body = "请求Flume接口[{$apis}]失败！";
        $this->sendmail($subject, $body);
    }

    public function mailLackField($logkey, $fieldStr) {
        $subject = "LogEs日志结构变更[{$logkey}]";
        $body = "日志[{$logkey}]结构有变更，缺少字段: {$fieldStr}，请及时更新ES结构! ";
        $this->sendmail($subject, $body);
    }

    public function mailNoDoc($logkey) {
        $subject = "LogEs日志结构变更[{$logkey}]";
        $body = "ES中，文档{$logkey}不存在，请及时更新ES结构! ";
        $this->sendmail($subject, $body);
    }

    public function sendmail($subject, $body) {
        $mails = Cfg::instance()->get('mail.mails');
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

