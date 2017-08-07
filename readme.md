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
            'enableSchemaCache' => true,
            'schemaCache' => 'cache',
            'schemaCacheDuration' => 86400
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
    
    // ====== insert data ORM ======
    
    $client->createCommand(null)
    ->insert('stat', [
        'event_data' => date('Y-m-d'),
        'counter_id' => 122
    ])
    ->execute();	
```

batch insert files
 
```php    	 	
    $files = [
        'dump_20170502' => Yii::getAlias('@app/dump_20170502.csv');
        'dump_20170503' => Yii::getAlias('@app/dump_20170503.csv');
        'dump_20170504' => Yii::getAlias('@app/dump_20170504.csv');
    ];	
    		
    $responses = $clickhouse->createCommand(null)
    ->batchInsertFiles('stat',null,[
        $files
    ],'CSV');	
    foreach($responses as $keyId => $response){
        var_dump($keyId . ' ' . $response->isOk);
    }	
    
```
batch insert files,  batch size = 100 lines
```php 
    $responses = $clickhouse->createCommand(null)
    ->batchInsertFilesDataSize('stat',null,[
        $files
    ],'CSV', 100);	
     foreach($responses as $keyId => $parts){
        foreach($parts as $partId => $response){
            var_dump($keyId . '_' . $partId. ' ' . $response->isOk);
        }
     }	

```
old methods: meta, rows, countAll, statistics 
```php     	
    $sql = 'SELECT 
        user_id, sum(income) AS sum_income
        FROM stat
        GROUP BY event_date
        WITH TOTALS
        LIMIT 10
    '; 	
    /** @var \kak\clickhouse\Connection $clickhouse */
    $clickhouse = \Yii::$app->clickhouse;
    
    $command = $clickhouse->createCommand($sql);  	
    $result  = $command->queryAll();
    
    var_dump($command->getMeta());  	      // columns meta info (columnName, dataType)
    var_dump($command->getMotals());         // result WITH TOTALS
    var_dump($command->getData());  	      // get rows data
    var_dump($command->getRows());  	      // rows count current result
    var_dump($command->getCountAll());       // rows count before limit at least	
    var_dump($command->getExtremes());  	
    var_dump($command->getStatistics());     // stat query 
    
 //or
     
    $command = $clickhouse->createCommand($sql);  
    $result  = $command->queryAll($command::FETCH_MODE_ALL);
    var_dump($result);
    
```
old examples ORM
```php

$q = (new \kak\clickhouse\Query())->from('stat')
    ->withTotals()
    ->where(['event_date' => '2017-05-01' , 'user_id' => 5 ])
    ->offset(2)
    ->limit(1);

$command = $q->createCommand();
$result  = $command->queryAll();
$total   = $command->getTotals();

var_dump($result);     // result data
var_dump($total);      // result WITH TOTALS

// -----

$command = (new \kak\clickhouse\Query())
    ->from('test_stat')
    ->withTotals();
    
$result =  $command->all();        // result data
var_dump($command->getTotals());      // result WITH TOTALS

```


set specific options 
```php
  /** @var \kak\clickhouse\Connection $client */
    $client = \Yii::$app->clickhouse;
    $sql = 'select * from stat where counter_id=:counter_id';
    $client->createCommand($sql,[
        ':counter_id' => 122
    ])->setOptions([
        'max_threads' => 2
    ])->queryAll();

// add options use method
// ->addOptions([])
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

Using SqlDataProvider
=====================
```php
$sql = 'select * from stat where counter_id=:counter_id and event_date=:date';
$provider = new \kak\clickhouse\data\SqlDataProvider([
    'db' => 'clickhouse',
    'sql' => $sql,
    'params' => [
        ':counter_id' => 1,
        ':date' => date('Y-m-d')
    ]
]);
```

Using Migration Data
=====================
<!--
convert schema mysql >>> clickhouse <br>
create custom console controller 
```php
    // ...
    public function actionIndex()
    {
        $exportSchemaCommand = new \kak\clickhouse\console\MigrationSchemaCommand([
            'sourceTable' => 'stat',
            'sourceDb' => \Yii::$app->db,
            'excludeSourceColumns' => [
                'id',
            ]
            'columns' => [
                '`event_date` Date' 
            ]
        ]);
        // result string SQL schema  
        $sql = $exportSchemaCommand->run();
        echo $sql;
    }    
``` -->
migration mysql,mssql data >>> clickhouse <br>
create custom console controller 
```php
  // ...
    public function actionIndex()
    {
        $exportDataCommand = new \kak\clickhouse\console\MigrationDataCommand([
            'sourceQuery' => (new Query())->select('*')->from('stat'),
            'sourceDb' => \Yii::$app->db,
            'storeTable' => 'test_stat',
            'storeDb' => \Yii::$app->clickhouse,
            'batchSize' => 10000,
            'filterSourceRow' => function($data){
                // if result false then skip save row
                $time = strtotime($data['hour_at']);
                return $time > 0;
            },
            'mapData' => [
                // key storeTable column => sourceTable column|call function 
                'event_date' => function($data){
                    return date('Y-m-d',strtotime($data['hour_at']));
                },
                'time' => function($data){
                    return strtotime($data['hour_at']);
                },
                'user_id' => 'partner_id'
            ]    
        ]);
        $exportDataCommand->run();  
     
    }
```
Result 
```text
php yii export-test/index

total count rows source table 38585
part data files count 4
save files dir: /home/user/test-project/www/runtime/clickhouse/stat
parts:
 >>> part0.data time 4.749
 >>> part1.data time 4.734
 >>> part2.data time 4.771
 >>> part3.data time 4.089
insert files
 <<< part0.data  time 3.289
 <<< part1.data  time 2.024
 <<< part2.data  time 1.938
 <<< part3.data  time 3.359
done
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
- 1 добавить приоброзование типов для неочень csv файлов
- 2 миграции из консольким 
- 4 ...
-->

Configuration tests IDE PHPStorm
================================
* 1 - download codecept & save current project root dir 
* http://codeception.com/quickstart
* 2 - menu -> Run -> Edit Configurations...
* 3 - press key `alt+insert` select codeception
* 4 - configuration form, view screen
<img src="https://hsto.org/web/877/998/cea/877998cea16e4e9bac8afd36dba5029c.png">


4 create config clickhouse `cd project_dir/public_html` & `touch vendor/kak/clickhouse/tests/_config/clickhouse.php`
```php
return [
    'class' => 'kak\clickhouse\Connection',
    'dsn' => '127.0.0.1',
    'port' => '8123',
    'username' => 'web',
    'password' => '123',
    'enableSchemaCache' => true,
    'schemaCache' => 'cache',
    'schemaCacheDuration' => 86400
];
```
