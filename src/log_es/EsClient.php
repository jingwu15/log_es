<?php
/**
 * @node_name ES客户端，主要用于查询
 * Desc: ES客户端基础服务
 * User: <lideqiang@yundun.com>
 * Date: 2018/9/27 9:47
 *
 * $result = EsClient::instance('only_for_test')->get(['name' => 'name_17']);
 * $result = EsClient::instance('only_for_test')->count(['name' => 'name_17']);
 * $result = EsClient::instance('only_for_test')->getsAll(['name' => 'name_17'], ["create_at" => "desc"]);
 * $result = EsClient::instance('only_for_test')->getsPage(['name' => 'name_17'], 1, 10, ["create_at" => "desc"]);
 */

namespace Jingwu\LogEs;

use Elasticsearch\ClientBuilder;

class EsClient extends Core {

    protected $_doc = '';
    static public $conn = null;
    static public $instances = [];

    public function __construct($doc = '') {
        $this->_doc = $doc;
    }

    static public function instance($doc = 'default') {
        if(!isset(self::$instances[$doc])) {
            self::$instances[$doc] = new self($doc);
        }
        return self::$instances[$doc];
    }

    protected function _cmd($cmd, $params = null) {
        $code = 0;
        $reasons = $result = [];
        try {
            $client = self::conn();
            $result = $params === null ? $client->$cmd() : $client->$cmd($params);
            $code = 1;
        } catch(\Exception $e) {
            $reasons = self::parseReason($e->getMessage()); 
        }
        return ['code' => $code, 'error'=>$reasons, 'data'=>$result];
    }

    public function getMap() {
        $code = 0;
        $reasons = $result = $map = [];
        $params = ['index' => $this->_doc];
        try {
            $result = self::conn()->indices()->getMapping($params);
            $tmp = current($result);
            if(isset($tmp['mappings'])) {
                $map = $tmp['mappings'];
                $code = 1;
                return ['code' => $code, 'error' => $reasons, 'data' => $map];
            } else {
                return ['code' => $code, 'error' => ["返回的数据中不存在 mappings"], 'data' => $map]
            }
        } catch(\Exception $e) {
            $reasons = self::parseReason($e->getMessage()); 
        }
        return ['code' => $code, 'error' => $reasons, 'data' => $map];
    }

    public function get($where) {
        $body    = $this->_parseWhere($where);
        $params  = ['index' => $this->_doc, 'body' => json_encode($body, JSON_UNESCAPED_UNICODE)];
        $result  = $this->_cmd('search', $params);
        $resultF = $this->_formatResult($result);
        if($resultF['status']) $resultF['data'] = $resultF['data'][0];
        return $resultF;
    }

    public function getsPage($where, $page, $pagesize, $sorts = null) {
        $body = $this->_parseWhere($where);
        if($sorts && is_array($sorts)) $body['sort'] = $sorts;
        $body["from"] = ($page - 1) * $pagesize;
        $body["size"] = $pagesize;
        $params = ['index' => $this->_doc, 'body' => json_encode($body, JSON_UNESCAPED_UNICODE)];
        $result = $this->_cmd('search', $params);
        return $this->_formatResult($result);
    }

    /**
     *  $sorts = ["create_at" => "asc"];
     *  $where = ['name' => 'name_17', "create_at" => ["gte" => "2018-09-21 16:49:06", "lte" => "2018-09-21 17:41:14"]];
     *  getsAll($where, $sorts);
     */
    public function getsAll($where, $sorts = null) {
        $body = $this->_parseWhere($where);
        if($sorts && is_array($sorts)) $body['sort'] = $sorts;
        $params = ['index' => $this->_doc, 'body' => json_encode($body, JSON_UNESCAPED_UNICODE)];
        $result = $this->_cmd('search', $params);
        return $this->_formatResult($result);
    }

    public function count($where) {
        $body = $this->_parseWhere($where);
        $params = ['index' => $this->_doc, 'body' => json_encode($body, JSON_UNESCAPED_UNICODE)];
        $result = $this->_cmd('count', $params);

        $resp = ["status" => 0, "error" => $result["error"], "total" => 0];
        if(!$result['error']) {
            $resp['status'] = 1;
            $resp['total'] = $result['data']['count'];
        }
        return $resp;
    }

    /*
     * [
     *      "ids => ["in" => [1, 2, 3]],
     *      "ids => ["not in" => [1, 2, 3]],
     *      "create_at => ["gte" => "2018-01-01 01:01:01", "lte" => "2018-01-01 04:01:01"],
     *      "name" => "jingwu",
     * ]
     */
    protected function _parseWhere($wheres) {
        $body = [];
        $body["query"] = [];
        $body["query"]["bool"] = [];
        $body["query"]["bool"]["must"] = [];
        foreach($wheres as $field => $value) {
            if(is_array($value)) {
                $keys = array_keys($value);
                if(in_array('in', $keys)) {
                    $body["query"]["bool"]["must"][] = ["terms" => [$field => $value]];
                } else if(in_array('not in', $keys)) {
                    $body["query"]["bool"]["must"][] = ["terms" => [$field => $value]];
                } else if(array_intersect(['gte', 'lte', 'gt', 'lt'], $keys)) {
                    $range = [];
                    foreach($keys as $key) {
                        if($key == 'gte') $range[$key] = $value[$key];
                        if($key == 'lte') $range[$key] = $value[$key];
                        if($key == 'gt' ) $range[$key] = $value[$key];
                        if($key == 'lt' ) $range[$key] = $value[$key];
                    }
                    $body["query"]["bool"]["must"][] = ["range" => [$field => $range]];
                } else {
                }
            } else {
                $body["query"]["bool"]["must"][] = ["term" => [$field => $value]];
            }
        }
        return $body;
    }

    protected function _formatResult($result) {
        $resp = ["status" => 0, "error" => $result["error"], "total" => 1, "data" => []];
        if(!$result["error"]) {
            $resp["status"] = 1;
            $resp["total"]  = $result["data"]["hits"]["total"];
            $resp["data"]   = $result["data"]["hits"]["hits"];
        }
        return $resp;
    }

    static function conn() {
        if(self::$conn == null) {
            $hosts = Cfg::instance()->get('es');
            self::$conn = ClientBuilder::create()
                ->setHosts($hosts)
                ->setConnectionPool('\Elasticsearch\ConnectionPool\StaticNoPingConnectionPool')
                ->setSelector('\Elasticsearch\ConnectionPool\Selectors\StickyRoundRobinSelector')
                ->setRetries(1)
                ->build();
        }
        return self::$conn;
    }

    static public function parseReason($raw = "") {
        $jdata = json_decode($raw, 1);
        return array_column($jdata['error']['root_cause'], 'reason');
    }

    public function __call($func, $params) {
        if(method_exists(self::conn(), $func)) {
            return $this->_cmd($func, $params[0]);
        }
    }

}
