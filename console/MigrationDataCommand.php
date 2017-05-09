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
    /** @var bool expand aggregate data to not aggregate */
    public $sourceRowExpandData = false;
    /** @var string table name to save data */
    public $storeTable;
    /** @var \kak\clickhouse\Connection */
    public $storeDb;
    /** @var int size data and step export data */
    public $batchSize = 10000;

    public $storeDir = '@app/runtime/clickhouse';

    /** @var array  'store_column' => 'source_column' */
    public $mapSchemaData = [];

    public function init()
    {
        parent::init();
        if($this->storeDb === null){
            $this->storeDb = \Yii::$app->clickhouse;
        }
    }

    public function run()
    {
        $dir = Yii::getAlias($this->storeDir) . "/". $this->sourceTable . '-to-' .$this->storeTable;
        if(!file_exists($dir)){
            echo "create dir " . $dir . "\n";
        }


    }

}