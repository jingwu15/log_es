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
            "jingwu/log_es": "0.2.5"
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

##### 2.6 配置 tmpdir/logpre
```
//tmpdir 是队列写入ES时，会生成临时文件，需要指定临时文件目录
Cfg::instance()->setTmpdir('/tmp');

//logpre 队列的默认前缀
Cfg::instance()->setLogpre('log_');
```

##### 2.7 使用conf文件配置
```
[log]
logdir = /tmp
logpre = log_

[beanstalk]
host = 127.0.0.1
port = 11300

[flume]
api0 = 127.0.0.1:51140
api1 = 127.0.0.1:51141
api2 = 127.0.0.1:51142
api3 = 127.0.0.1:51143
api3 = 127.0.0.1:51144

[es]
api0 = 127.0.0.1:9201
```

#### 3. 使用

注意：日志生成时，没有对文档格式进行严格校验，在写入ES时会校验，格式不匹配的，会邮件告警

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

//设置错误级别
LogClient::instance($logkey)->setLevel(LogClient::DEBUG);
//设置是否使用ES记录日志     0不使用   1使用
LogClient::instance($logkey)->useEs(1);
//设置是否使用文件记录日志, 文件默认为 /tmp/log_#logkey#.log     0不使用   1使用
LogClient::instance($logkey)->useFile(1);
//设置日志是否使用屏幕方录日志     0不使用   1使用
LogClient::instance($logkey)->useStdout(1);

////调用链方式
//LogClient::instance($logkey)->setLevel(LogClient::DEBUG)->useEs(1)->useFile(1)->useStdout(1);

LogClient::instance($logkey)->debug("this is debug");
LogClient::instance($logkey)->info("this is info");
LogClient::instance($logkey)->notice("this is notice");
LogClient::instance($logkey)->warn("this is warn");
LogClient::instance($logkey)->error("this is error");
```

#### 3.3 debug/info/notice/warn/error日志 添加自定义文件输出
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
LogClient::instance($logkey)->notice("this is notice");
LogClient::instance($logkey)->warn("this is warn");
LogClient::instance($logkey)->error("this is error");
```

#### 3.4 日志ES查询
```
$logkey = 'log_only_for_test';

$where  = ['_id' => 'AWX7gJtQKEvuT5mLCgIQ'];
$result = EsClient::instance($logkey)->get($where);
var_dump($result); 

$sorts  = ["create_at" => "asc"];
$where  = ['name' => 'name_17'];
$result = EsClient::instance($logkey)->getsAll($where, $sorts);
var_dump($result);

$sorts  = ["create_at" => "desc"];
$where  = ['name' => 'name_17', "create_at" => ["gte" => "2018-09-21 16:49:06", "lte" => "2018-09-21 17:41:14"]];
$result = EsClient::instance($logkey)->getsPage($where, 1, 100, $sorts);
var_dump($result);

$where  = ['name' => 'name_17'];
$result = EsClient::instance($logkey)->count($where);
var_dump($result);
```

#### 3.5 日志落地到ES
```
//beanstalk 中会有不同业务类型的队列，为了避免产生冲突，所有的日志队列统一设置前缀 log_
//如果不设置 prefix ，默认为 log_

//日志落地时，如果队列在ES中没有对应的文档，则会跳过，并报警；
//日志落地时，如果队列中的日志比ES中文档的结构有多出来的字段，需要更新ES萦引，会报警，日志会暂时写入文作；

//如果不指定前缀，则使用 log_
$prefix = "log_only_";
Flume::instance()->listen($prefix);

//日志写入队列时，因业务方面要求不严，有增加字段，而未更新ES时，会导致写入失败
//目前采用的机制时，先写入文件，等检测到ES更新时，再写入ES
Flume::instance()->correct($prefix);
```

