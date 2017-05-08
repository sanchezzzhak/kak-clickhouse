<?php
namespace kak\clickhouse;

use yii\db\Expression;
use yii\db\QueryBuilder as BaseQueryBuilder;

class QueryBuilder extends BaseQueryBuilder
{

    /**
     * Constructor.
     * @param Connection $connection the database connection.
     * @param array $config name-value pairs that will be used to initialize the object properties
     */
    public function __construct($connection, $config = [])
    {
        $this->db = $connection;
        parent::__construct($connection, $config);
    }

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

    private function prepareFromByModel($query)
    {
        if(empty($query->from) && $query instanceof ActiveQuery && !empty($query->modelClass)){
            $modelClass = $query->modelClass;
            $query->from = [ call_user_func($modelClass . '::tableName') ];
        }
    }



    /**
     * Generates a SELECT SQL statement from a [[Query]] object.
     * @param Query $query the [[Query]] object from which the SQL statement will be generated.
     * @param array $params the parameters to be bound to the generated SQL statement. These parameters will
     * be included in the result with the additional parameters generated during the query building process.
     * @return array the generated SQL statement (the first array element) and the corresponding
     * parameters to be bound to the SQL statement (the second array element). The parameters returned
     * include those provided in `$params`.
     */
    public function build($query, $params = [])
    {
        $query = $query->prepare($this);

        $params = empty($params) ? $query->params : array_merge($params, $query->params);

        $this->prepareFromByModel($query);

        $clauses = [
            $this->buildSelect($query->select, $params, $query->distinct, $query->selectOption),
            $this->buildFrom($query->from, $params),
            $this->buildSample($query->sample),
            $this->buildJoin($query->join, $params),
            $this->buildPreWhere($query->preWhere, $params),
            $this->buildWhere($query->where, $params),
            $this->buildGroupBy($query->groupBy),
            $this->buildHaving($query->having, $params),
            $this->buildWithTotals($query->hasWithTotals()),
        ];

        $sql = implode($this->separator, array_filter($clauses));
        $sql = $this->buildOrderByAndLimit($sql, $query->orderBy, $query->limit, $query->offset);

        if (!empty($query->orderBy)) {
            foreach ($query->orderBy as $expression) {
                if ($expression instanceof Expression) {
                    $params = array_merge($params, $expression->params);
                }
            }
        }
        if (!empty($query->groupBy)) {
            foreach ($query->groupBy as $expression) {
                if ($expression instanceof Expression) {
                    $params = array_merge($params, $expression->params);
                }
            }
        }

        $union = $this->buildUnion($query->union, $params);
        if ($union !== '') {
            $sql = "($sql){$this->separator}$union";
        }

        return [$sql, $params];
    }

    /**
     * @param string|array $condition
     * @return string the WITH TOTALS
     */
    public function buildWithTotals($condition)
    {
        return $condition === true ? ' WITH TOTALS ' : '';
    }

    /**
     * @param string|array $condition
     * @param array $params the binding parameters to be populated
     * @return string the PREWHERE clause built from [[Query::$preWhere]].
     */
    public function buildPreWhere($condition, &$params)
    {
        $where = $this->buildCondition($condition, $params);
        return $where === '' ? '' : 'PREWHERE ' . $where;
    }

    /**
     * @param string|array $condition
     * @return string the SAMPLE clause built from [[Query::$sample]].
     */
    public function buildSample($condition)
    {
        return $condition !== null ? ' SAMPLE ' . $condition : '';
    }

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

    /**
     * @param string|\yii\db\ColumnSchemaBuilder $type
     * @return mixed|string|\yii\db\ColumnSchemaBuilder
     */
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