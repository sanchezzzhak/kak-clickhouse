<?php
use kak\clickhouse\Connection as ClickhouseConnection;


return [
    'id' => 'test-clickhouse-console',
    'class' => 'yii\console\Application',
    'basePath' => \Yii::getAlias('@tests'),
    'runtimePath' => \Yii::getAlias('@tests/_output'),
    'bootstrap' => [],
    'components' => [
        'clickhouse' => array_merge([
            'class' => ClickhouseConnection::class,
            'dsn' => 'localhost',
            'port' => 8123,
            'database' => 'default',
            'username' => 'default',
            'password' => '',
            'enableSchemaCache' => false,
            'schemaCache' => 'cache',
            'schemaCacheDuration' => 604800
        ], is_file(__DIR__ . '/clickhouse.php') ? require __DIR__ . '/clickhouse.php' : []),
    ]
];
