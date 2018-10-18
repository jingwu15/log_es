<?php
namespace Jingwu\LogEs;

//1. post(url, data, header)                            //单独的URL
//1. setheads(headers)->post(url, data, headers)                 //公用的头
//$result = Curl::install()->post('http://www.ifeng.com');
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

    static $_instances = [];
    private $_headers  = [];
    private $_isZip = 0;
    public function __construct() {
    }

    static public function instance($key = 'default') {
        if(!isset(self::$_instances[$key])) {
            self::$_instances[$key] = new self($key);
        }
        return self::$_instances[$key];
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
            $data = is_string($data) ? gzencode($data) : gzencode(json_encode($data));
        }

        $curl = curl_init();
        curl_setopt($curl, CURLOPT_URL, $url);
        curl_setopt($curl, CURLOPT_POST, true);
        curl_setopt($curl, CURLOPT_HEADER, 1);
        curl_setopt($curl, CURLOPT_HTTPHEADER, $headers);
        curl_setopt($curl, CURLOPT_RETURNTRANSFER, 1);
        curl_setopt($curl, CURLOPT_POSTFIELDS, $data);
        $response = curl_exec($curl);
        curl_close($curl);
        return self::parseResponse($response);
    }

    public function head($url, $data = []) {
    }

    public function delete($url, $data = []) {
    }

    static public function parseResponse($response) {
        list($headerRaw, $body) = explode("\r\n\r\n", $response, 2);
        list($first, $headerArr) = explode("\r\n", $headerRaw, 2);
        list($version, $code, $codeTitle) = explode(' ', $first, 3);
        $headers = [];
        $headerLines = explode("\r\n", $headerArr);
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
$result = Curl::install()->post('http://www.ifeng.com');        print_r($result);
$result = Curl::install()->get('http://www.ifeng.com');         print_r($result);
$result = Curl::install()->putt('http://www.ifeng.com');        print_r($result);
$result = Curl::install()->head('http://www.ifeng.com');        print_r($result);
$result = Curl::install()->delete('http://www.ifeng.com');      print_r($result);
$result = Curl::install()->patch('http://www.ifeng.com');       print_r($result);
$result = Curl::install()->get('http://www.ifeng.com');         print_r($result);
$result = Curl::install()->get('http://www.ifeng.com');         print_r($result);
 */

