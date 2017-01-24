<?php
namespace kak\clickhouse;

use yii\db\QueryBuilder as BaseQueryBuilder;

class QueryBuilder extends BaseQueryBuilder
{
    /**
     * Clickhouse data types
     */
    public $typeMap = [
        Schema::TYPE_CHAR => 'FixedString(1)',
        Schema::TYPE_STRING => 'String',
        Schema::TYPE_TEXT => 'String',
        Schema::TYPE_SMALLINT => 'Int8',
        Schema::TYPE_INTEGER => 'Int32',
        Schema::TYPE_BIGINT => 'Int64',
        Schema::TYPE_FLOAT => 'Float32',
        Schema::TYPE_DOUBLE => 'Float64',
        Schema::TYPE_DECIMAL => 'Float32',
        Schema::TYPE_DATETIME => 'DateTime',
        Schema::TYPE_TIME => 'DateTime',
        Schema::TYPE_DATE => 'Date',
        Schema::TYPE_BINARY => 'String',
        Schema::TYPE_BOOLEAN => 'Int8',
        Schema::TYPE_MONEY => 'Float32',
    ];

    /**
     * Set default engine option if don't set
     *
     * @param $table
     * @param $columns
     * @param null $options
     * @return mixed
     */
    public function createTable($table, $columns, $options = null)
    {
        if ($options === null) {
            $options = 'ENGINE=Memory';
        }
        return parent::createTable($table, $columns, $options);
    }

    public function getColumnType($type)
    {
        if ($type instanceof ColumnSchemaBuilder) {
            $type = $type->__toString();
        }

        if (isset($this->typeMap[$type])) {
            return $this->typeMap[$type];
        } elseif (preg_match('/^(\w+)\s+/', $type, $matches)) {
            if (isset($this->typeMap[$matches[1]])) {
                return preg_replace('/^\w+/', $this->typeMap[$matches[1]], $type);
            }
        } elseif (preg_match('/^U(\w+)/', $type, $matches)) {
            if (isset($this->typeMap[$matches[1]])) {
                return 'U' . $this->typeMap[$matches[1]];
            }
        }

        return $type;
    }

    /**
     * @param integer $limit
     * @param integer $offset
     * @return string the LIMIT and OFFSET clauses
     */
    public function buildLimit($limit, $offset)
    {
        $sql = '';
        if ($this->hasOffset($offset)) {
            $sql .= 'LIMIT ' . $offset . ' , ' . $limit;
        }
        else if ($this->hasLimit($limit)) {
            $sql = 'LIMIT ' . $limit;
        }

        return ltrim($sql);
    }


}