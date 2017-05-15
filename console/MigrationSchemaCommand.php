<?php
/**
 * Created by PhpStorm.
 * User: kak
 * Date: 09.05.2017
 * Time: 12:59
 */
namespace kak\clickhouse\console;
use kak\clickhouse\Schema;
use yii\base\Object;
use Yii;


class MigrationSchemaCommand extends Object
{
    const RESULT_TYPE_SQL = 1;
    const RESULT_TYPE_MIGRATION = 2;

    /** @var string source table name */
    public $sourceTable;
    /** @var \yii\db\Connection */
    public $sourceDb;
    public $columns = [
        'event_date Date'
    ];

    public $excludeSourceColumns = [
        'id'
    ];
    /** @var \kak\clickhouse\Connection */
    public $storeDb;

    public function init()
    {
        parent::init();
        if($this->storeDb === null){
            $this->storeDb = \Yii::$app->clickhouse;
        }
    }


    private function getConvertTypeToClickHouseType(yii\db\ColumnSchema $column)
    {
        $size = $column->size;
        $unsigned = $column->unsigned ? 'U':'';
        $type = $column->type;

        switch ($type){
             case Schema::TYPE_BIGINT:
                 return $unsigned."Int64";
             case Schema::TYPE_INTEGER:
                 $typeSize = ($size > 16) ? 32 : 16;
                 return $unsigned."Int".$typeSize;
            case Schema::TYPE_SMALLINT:
                return $unsigned."Int16";
            case Schema::TYPE_BOOLEAN:
                return $unsigned."Int8";
            case Schema::TYPE_MONEY:
            case Schema::TYPE_DECIMAL:
                $typeSize = $column->precision > 32 ? 64: 32;
                return "Float".$typeSize;
            case Schema::TYPE_TEXT:
            case Schema::TYPE_STRING:
                if($size < 100) {
                    return "FixedString({$size})";
                }
                return "String";
            case Schema::TYPE_TIMESTAMP :
            case Schema::TYPE_DATETIME :
                return "DateTime";
            case Schema::TYPE_DATE:
                return "Date";
        }
        return "";
    }

    /**
     * Get sql schema mysql >  clickhouse table
     * @return bool|string
     */
    public function run()
    {
        if(!$table = Yii::$app->getDb()->getTableSchema($this->sourceTable, true)){
            return false;
        }

        $sql = 'CREATE TABLE IF NOT EXISTS `'. $this->sourceTable . '` ( ' . "\n";
        $columns = [];
        foreach ($table->columns as $column){
            if(in_array($column->name, $this->excludeSourceColumns)){
                continue;
            }
            $type = $this->getConvertTypeToClickHouseType($column);
            $columns[] = '`'.$column->name . '` ' . $type;
        }
        $columns = array_merge($this->columns,$columns);

        $sql.=implode(",\n",$columns);
        $sql.="\n)";

        return $sql;
    }

}