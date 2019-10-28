<?php
namespace Jingwu\LogEs;

//1. post(url, data, header)                            //单独的URL
//1. setheads(headers)->post(url, data, headers)                 //公用的头
//$result = Curl::instance()->post('http://www.ifeng.com');
//{
//    "version": "",
//    "code": "",
//    "code_title": "",
//    "headers" : {
//          "Server": "Cdn Cache Server V2.0",
//          "Date": "Fri, 21 Sep 2018 03:41:28 GMT"
//    },
//    "body" : "<!DOCTYPE html><html><head></head><body></body></html>"
//}

class Curl extends Core {

    private $_isZip = 0;
    private $_headers  = [];
    static public $instances = [];
    public function __construct() {
    }

    static public function instance($key = 'default') {
        if(!isset(self::$instances[$key])) {
            self::$instances[$key] = new self($key);
        }
        return self::$instances[$key];
    }

    public function setHeadersCommon($headers = []) {
        foreach($headers as $key => $value) {
            $this->_headers[$key] = $value;
        }
        return $this;
    }

    public function clearHeadersCommon() {
        $this->_headers = [];
        return $this;
    }

    public function setZip($flag = 1) {
        $this->_isZip = $flag;
        $this->_headers['Content-Encoding'] = 'gzip';
        return $this;
    }

    public function get($url, $data = [], $headers = []) {
        $headers = array_merge($this->_headers, $headers);
        $curl = curl_init();
        curl_setopt($curl, CURLOPT_URL, $url);
        curl_setopt($curl, CURLOPT_HEADER, 1);
        curl_setopt($curl, CURLOPT_HTTPHEADER, $headers);
        curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1);
        $response = curl_exec($curl);
        curl_close($curl);
        return self::parseResponse($response);
    }

    public function put($url, $data = []) {
    }

    public function post($url, $data = [], $headers = []) {
        $headers = array_merge($this->_headers, $headers);
        if($this->_isZip) {
            $headers['Content-Encoding'] = 'gzip';
            //$headers['Accept-Encoding'] = 'gzip';
            $data = is_string($data) ? gzencode($data) : gzencode(json_encode($data, JSON_UNESCAPED_UNICODE));
        }

        $curl = curl_init();
        curl_setopt($curl, CURLOPT_URL, $url);
        curl_setopt($curl, CURLOPT_POST, true);
        curl_setopt($curl, CURLOPT_HEADER, 1);
        curl_setopt($curl, CURLOPT_HTTPHEADER, $headers);
        curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($curl, CURLOPT_POSTFIELDS, $data);
        $response = curl_exec($curl);
        $error = curl_error($curl);
        curl_close($curl);

        $resp = ['version' => 'HTTP/1.1', 'code' => 0, 'code_title' => '', 'headers' => [], 'body' => '', 'error' => $error];
        if($response) {
            return array_merge($resp, self::parseResponse($response));
        } else {
            return $resp;
        }
    }

    public function head($url, $data = []) {
    }

    public function delete($url, $data = []) {
    }

    static public function parseResponse($response) {
        $headers = [];
        list($headerRaw, $body) = explode("\r\n\r\n", $response, 2);
        $headerLines = explode("\r\n", $headerRaw);
        $first       = array_shift($headerLines);
        list($version, $code, $codeTitle) = explode(' ', $first, 3);
        foreach($headerLines as $line) {
            list($key, $value) = explode(": ", $line, 2);
            $headers[$key]     = $value;
        }
        return [
            'version'    => $version,
            'code'       => $code,
            'code_title' => $codeTitle,
            'headers'    => $headers,
            'body'       => $body,
        ];
    }

}




/**
$result = Curl::instance()->post('http://www.ifeng.com');        print_r($result);
$result = Curl::instance()->get('http://www.ifeng.com');         print_r($result);
$result = Curl::instance()->putt('http://www.ifeng.com');        print_r($result);
$result = Curl::instance()->head('http://www.ifeng.com');        print_r($result);
$result = Curl::instance()->delete('http://www.ifeng.com');      print_r($result);
$result = Curl::instance()->patch('http://www.ifeng.com');       print_r($result);
$result = Curl::instance()->get('http://www.ifeng.com');         print_r($result);
$result = Curl::instance()->get('http://www.ifeng.com');         print_r($result);
 */

