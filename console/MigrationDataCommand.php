<?php
/**
 * Created by PhpStorm.
 * User: kak
 * Date: 09.05.2017
 * Time: 12:25
 */
namespace kak\clickhouse\console;
use yii\base\Object;
use Yii;

use yii\helpers\FileHelper;

/**
 * Class MigrationDataCommand
 * @package kak\clickhouse\console
 */
class MigrationDataCommand extends Object
{
    /** @var string source table name */
    public $sourceTable;
    /** @var \yii\db\Connection */
    public $sourceDb;
    /** @var bool expand aggregate data to not aggregate save */
    public $sourceRowExpandData = false;
    /** @var string table name to save data */
    public $storeTable;
    /** @var \kak\clickhouse\Connection */
    public $storeDb;
    /** @var int size data and step export data */
    public $batchSize = 10000;


    /** @var array  'store_column' => 'source_column' */
    public $mapSchemaData = [

    ];

    public function init()
    {
        parent::init();
        if($this->storeDb === null){
            $this->storeDb = \Yii::$app->clickhouse;
        }
    }


    private function arrayToValueStr($row)
    {
        $out = [];

        foreach ($this->mapSchemaData as $key => $map){
            $val = '';
            if($map instanceof \Closure){
                $val = call_user_func_array($map,$row);
            }
            $out[$key] = $row;
        }
        return implode(',',$out);
    }

    private $_schema;


    public function run()
    {
        $dir = Yii::getAlias('@app/runtime/clickhouse') . "/". $this->sourceTable . '-to-' .$this->storeTable;
        if(!file_exists($dir)){
            echo "create dir " . $dir . "\n";
            FileHelper::createDirectory($dir);
        }

        $this->_schema = $this->storeDb->getTableSchema($this->storeTable,true);
        //var_dump($this->_schema);

        exit;
        $countTotal = (new \yii\db\Query())->from($this->sourceTable)->limit(1)->count('id',$this->sourceDb);
        echo "total count rows source table {$countTotal}\n";
        $partCount = ceil($countTotal/ $this->batchSize);
        echo "part data files count {$partCount}\n";
        $partCount = 2;


        $files = [];
        for($i=0; $i < $partCount; $i++) {
            $offset = ($i) * $this->batchSize;
            $rows = (new \yii\db\Query())->from($this->sourceTable)
                ->limit($this->batchSize)
                ->offset($offset)
                ->all($this->sourceDb);

            $path = $dir . '/part' . $i. '.data';
            $lines = '';
            foreach ($rows as $row){
                $lines.="" . $this->arrayToValueStr($row) . "\n";
            }
            var_dump('111',$lines);

            exit;
            $files[] = $path;
            file_put_contents($path,$lines);
        }
    }

}