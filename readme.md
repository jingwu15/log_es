### 日志基础服务

### 1. 介绍
```
日志基础服务分为4个部分：

日志生成: 日志分为2类，常规日志与业务日志；
    常规日志: debug/info/warn/error等不同级别的日志。
    业务日志: 没有级别，只有时间，请求数据，动作，执行结果。

日志写入队列：日志存储基于ES，为了提高写入性能，先把数据写入到队列，然后批量写入ES

日志写入到ES：队列数据批量写入ES，并对请求进行压缩

日志查询：对日志的查询有2个业务场景，非业务查询与业务查询。
    非业务查询: 指仅通过ES的工具/接口等即满足需求。
    业务需求指: 在业务系统中需要数据的处理 / 展示, 可使用原生的ES查询。

```


#### 2. 配置

##### 2.1 版本及依赖
-----------------------
0.1.1, PHP >=5.4.0 (in progress)

##### 2.2 使用composer
-----
添加依赖 ``jingwu/log_es`` 到项目的 ``composer.json`` 文件:
```json
    {
        "require": {
            "jingwu/log_es": "0.1.2"
        }
    }
```

```
require "vendor/autoload.php";

use Jingwu\LogEs\Cfg;
use Jingwu\LogEs\Flume;
use Jingwu\LogEs\LogQueue;
use Jingwu\LogEs\LogClient;
use Jingwu\LogEs\EsClient;
```

##### 2.3 配置Beanstalk
```
$configBeanstalk = [
    "host" => "127.0.0.1",
    "port" => 11300,
];
Cfg::instance()->setBeanstalk($configBeanstalk["host"], $configBeanstalk["port"]);
```

##### 2.4 配置Flume
```
//Flume支持多个配置，在使用时会隋机选择
$configFlume = [
    'http://127.0.0.1:50000',
    'http://127.0.0.1:50001',
];
Cfg::instance()->setFlume($configFlume);
```

##### 2.5 配置Es
```
//ES支持多个配置，在使用时会隋机选择
$configEs = [
    '127.0.0.1:9001',
    '127.0.0.1:9002',
];
Cfg::instance()->setEs($configEs);
```

##### 2.6 配置logs
```
//logs 是每种日志的字段配置
$configLogs = [
    'log_only_for_test' => 'key, name, title, create_at',
    'log_only_loginfo' => 'datetime, channel, level, level_name, context, message, formatted, extra',
];
Cfg::instance()->setLogs($configLogs);
```

#### 3. 使用

#### 3.1 业务型日志生成
```
$logkey = 'log_only_for_test';
$data = [
    "key"       => "key_{$i}",
    "name"      => "name_{$i}",
    "title"     => "title_{$i}",
    "create_at" => date("Y-m-d H:i:s"),
];
LogClient::instance($logkey)->add($data);
```

#### 3.2 debug/info/warn/error日志生成
```
$logkey = 'log_only_info';

LogClient::instance($logkey)->debug("this is debug");
LogClient::instance($logkey)->info("this is info");
LogClient::instance($logkey)->warn("this is warn");
LogClient::instance($logkey)->error("this is error");
```

#### 3.3 debug/info/warn/error日志 添加文件输出
```
$logkey = 'log_only_info';
$logfile = '/tmp/log_test_loges.log';

//创建新的logger
$logger = new \Monolog\Logger($logkey);

//日志写入到文件
$streamHandler = new \Monolog\Handler\StreamHandler($logfile, \Monolog\Logger::DEBUG);
$streamHandler->setFormatter(new \Monolog\Formatter\LineFormatter("[%datetime%] %channel%.%level_name%: %message% %context% %extra% \n", '', true));
$logger->pushHandler($streamHandler);

//使用 php 配置，受 php 的 display_errors, error_reporting 及 error_log 影响
$errorHandler = new Monolog\Handler\ErrorLogHandler(4, \Monolog\Logger::ERROR, true, true);
$errorHandler->setFormatter(new \Monolog\Formatter\LineFormatter("[%datetime%] %channel%.%level_name%: %message% %context% %extra% \n", '', true));
$logger->pushHandler($errorHandler);

//添加到 LogClient 中
LogClient::setLogger($logkey, $logger);

//正常的使用 LogClient
LogClient::instance($logkey)->debug("this is debug");
LogClient::instance($logkey)->info("this is info");
LogClient::instance($logkey)->warn("this is warn");
LogClient::instance($logkey)->error("this is error");
```

#### 3.4 日志ES查询
```
$logkey = 'log_only_for_test';

//getsAll  取得全部数据
$sorts  = ["create_at" => "asc"];
$where  = ['name' => 'name_17', "create_at" => ["gte" => "2018-09-21 16:49:06", "lte" => "2018-09-21 17:41:14"]];
$result = EsClient::instance($logkey)->getsAll($where, $sorts);
var_dump($result); exit();

//getsPage  按页取得数据
$sorts  = ["create_at" => "desc"];
$where  = ['name' => 'name_17'];
//$where  = ['name' => 'name_17', "create_at" => ["gte" => "2018-09-21 16:49:06", "lte" => "2018-09-21 17:41:14"]];
$result = EsClient::instance($logkey)->getsPage($where, 1, 100, $sorts);
var_dump($result); exit();

//get   取得单条数据
$where  = ['_id' => 'AWX7gJtQKEvuT5mLCgIQ'];
$result = EsClient::instance($logkey)->get($where);
var_dump($result); exit();

//count   统计计录数
$where  = ['name' => 'name_17'];
$result = EsClient::instance($logkey)->count($where);
var_dump($result); exit();
```

#### 3.5 日志落地到ES
```
//beanstalk 中会有不同业务类型的队列，为了避免产生冲突，所有的日志队列统一设置前缀 log_
//如果不设置 prefix ，默认为 log_

$prefix = "log_only_";
Flume::instance()->listen($prefix);
```

