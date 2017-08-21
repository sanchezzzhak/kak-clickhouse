<?php
/**
 * Created by PhpStorm.
 * User: kak
 * Date: 09.05.2017
 * Time: 12:25
 */

namespace kak\clickhouse\console;

use kak\clickhouse\ColumnSchema;
use kak\clickhouse\TableSchema;
use yii\base\Object;
use Yii;

use yii\helpers\FileHelper;
use yii\helpers\VarDumper;

/**
 * Class MigrationDataCommand
 * @package kak\clickhouse\console
 */
class MigrationDataCommand extends Object
{
    const FORMAT_CSV = 'CSV';
    const FORMAT_JSON_EACH_ROW = 'JSONEachRow';

    /** @var string source table name */
    public $sourceTable;
    /** @var \yii\db\Query */
    public $sourceQuery;
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
    public $format = self::FORMAT_CSV;

    /** @var \Closure($row) if closure return false then skip row save dump */
    public $filterSourceRow;

    /** @var array  'store_column' => 'source_column' */
    public $mapData = [

    ];

    /** @var  TableSchema */
    private $_schema;
    private $_files = [];

    /**
     * @return array
     */
    public function getFiles()
    {
        return $this->_files;
    }

    /**
     * @param array $files
     */
    public function setFiles($files)
    {
        $this->_files = $files;
    }

    public function init()
    {
        parent::init();
        if ($this->storeDb === null) {
            $this->storeDb = \Yii::$app->clickhouse;
        }
    }

    /**
     * @param $row
     * @return string
     */
    private function prepareExportData($row)
    {
        $out = [];
        foreach ($this->mapData as $key => $item) {
            $val = 0;
            if ($item instanceof \Closure) {
                $val = call_user_func($item, $row);
                $val = $this->castTypeValue($key, $val);
            } elseif (is_string($item) && isset($row[$item])) {
                $val = $this->castTypeValue($key, $row[$item]);
            } else if (isset($this->_schema->columns[$key])) {
                $val = $this->castTypeValue($key, $item);
            }
            $out[$key] = $val;
        }

        if ($this->format == self::FORMAT_JSON_EACH_ROW) {
            return json_encode($out);
        }

        return implode(',', $out);
    }

    private function castTypeValue($key, $val)
    {
        $column = isset($this->_schema->columns[$key]) ? $this->_schema->columns[$key] : null;
        if ($column !== null) {
            $val = $this->storeDb->quoteValue($column->phpTypecast($val));
        }
        return $val;
    }


    /**
     * get total records source table
     * @return int|string
     */
    private function getTotalRows()
    {
        if ($this->sourceQuery !== null) {
            $query = clone $this->sourceQuery;
            return $query->limit(1)->count('*', $this->sourceDb);
        }
        return (new \yii\db\Query())->from($this->sourceTable)->limit(1)->count('*', $this->sourceDb);
    }

    /**
     * get records source table
     * @param $offset
     * @return array
     */
    private function getRows($offset)
    {
        if ($this->sourceQuery !== null) {
            $query = clone $this->sourceQuery;
            return $query->limit($this->batchSize)
                ->offset($offset)
                ->all($this->sourceDb);
        }

        return (new \yii\db\Query())->from($this->sourceTable)
            ->limit($this->batchSize)
            ->offset($offset)
            ->all($this->sourceDb);
    }

    /**
     * @throws \yii\base\Exception
     */
    private function checkTableSchema()
    {
        if (!$this->_schema = $this->storeDb->getTableSchema($this->storeTable, true)) {
            throw new \yii\base\Exception('ClickHouse: table `' . $this->storeTable . '` not found');
        }

        // checks columns in table
        $columns = array_keys($this->mapData);
        $columnsNotFound = [];
        foreach ($columns as $columnName) {
            if (!isset($this->_schema->columns[$columnName])) {
                $columnsNotFound[] = $columnName;
            }
        }

        if (count($columnsNotFound) > 0) {
            throw new \yii\base\Exception('ClickHouse: table `' . $this->storeTable . '` columns not found  (' . implode(',', $columnsNotFound) . ')');
        }

    }

    /**
     * load last files dump
     * @return bool
     */
    public function loadDump()
    {
        $dir = Yii::getAlias('@app/runtime/clickhouse') . $this->storeTable;
        if (!file_exists($dir)) {
            echo "dir not exist {$dir}\n";
            return false;
        }
        $this->checkTableSchema();
        $files = FileHelper::findFiles($dir, ['recursive' => false]);
        $this->setFiles($files);
    }

    /**
     * save source-table data to files
     * @return array
     */
    public function saveDump()
    {
        $dir = Yii::getAlias('@app/runtime/clickhouse') . "/" . $this->storeTable;
        if (!file_exists($dir)) {
            echo "create dir {$dir}\n";
            FileHelper::createDirectory($dir);
        }

        $this->checkTableSchema();
        $countTotal = $this->getTotalRows();

        $useFilterSourceRow = $this->filterSourceRow !== null && $this->filterSourceRow instanceof \Closure;

        echo "total count rows source table {$countTotal}\n";
        $partCount = ceil($countTotal / $this->batchSize);
        echo "part data files count {$partCount}\n";
        echo "save files dir: {$dir} \n";
        echo "parts:\n";

        $this->_files = [];
        for ($i = 0; $i < $partCount; $i++) {
            $timer = microtime(true);
            $file = 'part' . $i . '.data';
            $path = $dir . '/' . $file;

            $offset = ($i) * $this->batchSize;
            $rows = $this->getRows($offset);
            $lines = '';
            foreach ($rows as $row) {
                if ($useFilterSourceRow && !call_user_func($this->filterSourceRow, $row)) {
                    continue;
                }
                $lines .= $this->prepareExportData($row) . "\n";
            }
            $this->_files[] = $path;
            file_put_contents($path, $lines);

            $timer = sprintf('%0.3f', microtime(true) - $timer);
            echo " >>> " . $file . " time {$timer} \n";
        }

        return $this->_files;
    }

    /**
     * import data clickhouse
     */
    public function importDump()
    {
        $files = $this->_files;
        if (count($files)) {
            $keys = [];

            foreach ($this->mapData as $key => $item) {
                $keys[] = $key;
            }
            echo "insert files \n";
            foreach ($files as $file) {
                $timer = microtime(true);
                $responces = $this->storeDb->createCommand()->batchInsertFiles($this->storeTable, $keys, [$file], $this->format);
                $timer = sprintf('%0.3f', microtime(true) - $timer);
                $partName = pathinfo($file, PATHINFO_BASENAME);
                if (!$responces[0]->getIsOk()) {
                    echo " !!! " . $partName . " time {$timer} error: " . $responces[0]->getContent();
                    continue;
                }
                echo " <<< " . $partName . " time {$timer}\n";
            }
        }

        echo "done\n";
    }


    /**
     * 1 export data mysql
     * 2 import data clickhouse
     * @param bool $insert
     */
    public function run($insert = true)
    {
        $this->saveDump();
        if ($insert) {
            $this->importDump();
        }
    }

}