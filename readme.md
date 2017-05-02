# Yii2 ClickHouse extension

## Installation

### Composer
The preferred way to install this extension is through [Composer](http://getcomposer.org/).

Either run

	php composer.phar require kak/clickhouse "dev-master"

or add

	"kak/clickhouse": "dev-master"

to the require section of your composer.json


## Configuration example
```php  
   'components' => [
        'clickhouse' => [
            'class' => 'kak\clickhouse\Connection',
            'dsn' => '127.0.0.1',
            'port' => '8123',
           // 'database' => 'default',  // use other database name
            'username' => 'web',
            'password' => '123',
        ],
   // ...     
```

## Notes
* If clickhouse server responds with no response == 200, then you will get the exception

## Usage
```php    
   /** @var \kak\clickhouse\Connection $client */
    $client = \Yii::$app->clickhouse;
    $sql = 'select * from stat where counter_id=:counter_id';
    $client->createCommand($sql,[
        ':counter_id' => 122
    ])->queryAll();


    // insert data ORM
    $client->createCommand(null)
    ->insert('stat', [
        'event_data' => date('Y-m-d'),
        'counter_id' => 122
    ])
    ->execute();
			
```
Save custom model 
```php

use yii\base\Model;
class Stat extends Model
{
    public $event_date; // Date;
    public $counter_id  = 0; // Int32,

	public function save($validate = true)
	{
		/** @var \kak\clickhouse\Connection $client */
		$client = \Yii::$app->clickhouse;
		$this->event_date = date('Y-m-d');

		if($validate && !$this->validate()){
			return false;
		}

		$attributes = $this->getAttributes();
		$client->createCommand(null)
			->insert('stat', $attributes )
			->execute();	
	}

}
```

## ActiveRecord model

```php
class Stat extends \kak\clickhouse\ActiveRecord 
{

    // pls overwrite method is config section !=clickhouse
    // default clickhouse
	public static function getDb()
	{
	    return \Yii::$app->clickhouse;
	}


    public static function tableName()
    {
        return 'stat';
    }
    
    // use relation in mysql (Only with, do not use joinWith)
    
    public function getUser()
    {
    	return $this->hasOne(User::className(),['id' => 'user_id']);
    }
}
```

Using Gii generator
===================
```php
return [
    //....
    'modules' => [
        // ...
        'gii' => [
            'class' => 'yii\gii\Module',
            'allowedIPs' => [
                        '127.0.0.1',
                        '::1',
                        '192.168.*',
                        '10.*',
            ],
            'generators' => [
                'clickhouseDbModel' => [
                    'class' => 'kak\clickhouse\gii\model\Generator'
                ]
            ],
        ],
    ]
];
```
Using Debug panel
===================

```
$config['bootstrap'][] = 'debug';
    $config['modules']['debug'] = [
        'class' => 'yii\debug\Module',
        'allowedIPs' => [
            '127.0.0.1',
            '::1',
            '192.168.*',
            '10.*',
        ],
        'panels' => [
            'clickhouse' => [
                'class' => 'kak\clickhouse\debug\Panel',
                'db' => 'clickhouse'
            ],
        ]

    ];
```

ClickHouse Reference Manual
===================
https://clickhouse.yandex/reference_en.html


Summary of recommendations insert data
===================
- 1 Accumulated data and insert at one time, it will reduce the operations io disk 
- 2 @todo how that will add...


<!--
@todo сделать в планах
- 1 вставку большого файла
- 2 добавить приоброзование типов для неочень csv файлов
- 3 миграции из консольким 
- 4 ...
-->