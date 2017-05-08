<?php
return [
    'id' => 'test-clickhouse-console',
    'class' => 'yii\console\Application',
    'basePath' => \Yii::getAlias('@tests'),
    'runtimePath' => \Yii::getAlias('@tests/_output'),
    'bootstrap' => [],
    'components' => [
        'clickhouse' => require ('clickhouse.php'),
    ]
];