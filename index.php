<?php

require_once __DIR__ . '/vendor/autoload.php';

use Rx\Observable;
use React\EventLoop\Factory;
use Rx\Scheduler;


$rk = new RdKafka\Producer();
$rk->setLogLevel(LOG_DEBUG);
$rk->addBrokers("kafka");

$ptopic = $rk->newTopic("test");




$conf = new RdKafka\Conf();

// Set the group id. This is required when storing offsets on the broker
$conf->set('group.id', 'myConsumerGroup');

$rk = new RdKafka\Consumer($conf);
$rk->addBrokers("kafka");

$topicConf = new RdKafka\TopicConf();
$topicConf->set('auto.commit.interval.ms', 100);

// Set the offset store method to 'file'
$topicConf->set('offset.store.method', 'file');
$topicConf->set('offset.store.path', sys_get_temp_dir());

// Alternatively, set the offset store method to 'broker'
// $topicConf->set('offset.store.method', 'broker');

// Set where to start consuming messages when there is no initial offset in
// offset store or the desired offset is out of range.
// 'smallest': start from the beginning
$topicConf->set('auto.offset.reset', 'smallest');

$topic = $rk->newTopic("test", $topicConf);

// Start consuming partition 0
$topic->consumeStart(0, RD_KAFKA_OFFSET_STORED);









$loop = Factory::create();

//You only need to set the default scheduler once
Scheduler::setDefaultFactory(function() use($loop){
    return new Scheduler\EventLoopScheduler($loop);
});


Observable::
    range(1, 10)->
    interval(500)->
    take(20)->
    subscribe(function($k) use ($ptopic, $rk) {
        $ptopic->produce(RD_KAFKA_PARTITION_UA, 0, "Message $k");
        $rk->poll(0);
        echo $k . PHP_EOL;
    });

Observable::interval(300)->
    flatMap(function ($i) use ($topic) {
        $message = $topic->consume(0, 120*10000);
        switch ($message->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                return Observable::of($message);
            break;
            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                return Observable::of((object)['payload' => "No more messages; will wait for more\n"]);
            break;
            case RD_KAFKA_RESP_ERR__TIMED_OUT:
                return Observable::of((object)['payload' => "Timed out\n"]);
            break;
            default:
                throw new \Exception($message->errstr(), $message->err);
            break;
        }

    })->
    subscribe(function ($e) {
        echo $e->payload, PHP_EOL;
    });

$loop->run();
