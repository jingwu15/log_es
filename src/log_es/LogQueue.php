<?php
/**
 * @node_name 日志基础服务
 * Desc: 日志基础服务
 * User: <lideqiang@yundun.com>
 * Date: 2018/9/27 9:47
 *
 * 服务依赖于 jingwu/phpbeanstalk 0.1.3
 */
namespace Jingwu\LogEs;

use Jingwu\PhpBeanstalk\Client as BsClient;

class LogQueue extends BsClient {

    static protected $_instances = [];

    public function __construct($config = []) {
        parent::__construct($config);
    }

    static public function instance($key = 'default') {
        if(!isset(self::$_instances[$key])) {
            $config = Cfg::instance()->get('beanstalk');
            self::$_instances[$key] = new self($config);
        }
        return self::$_instances[$key];
    }

}

