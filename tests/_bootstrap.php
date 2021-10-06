<?php
defined('YII_DEBUG') or define('YII_DEBUG', true);
defined('YII_ENV') or define('YII_ENV', 'dev');

$autoloadGitHub = dirname(__DIR__, 1) . '/vendor/autoload.php';
$yiiGitHub = dirname(__DIR__, 1) . '/vendor/yiisoft/yii2/Yii.php';

require_once $yiiGitHub;
require_once $autoloadGitHub;

Yii::setAlias('@tests', __DIR__);
Yii::setAlias('@data', __DIR__ . DIRECTORY_SEPARATOR . '_data');
