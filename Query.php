<?php
/**
 * Created by PhpStorm.
 * User: kak
 * Date: 03.05.2017
 * Time: 11:53
 */
namespace kak\clickhouse;

use yii\db\Query as BaseQuery;
use yii\db\Exception as DbException;


/**
 * Class Query
 * @package kak\clickhouse
 * @method getCountAll() int
 * @method getTotals() array
 * @method getData() array
 * @method getExtremes() array
 * @method getRows() int
 * @method getMeta() array
 */
class Query extends BaseQuery
{

    /** @var \kak\clickhouse\Command|null  */
    private $_command;
    /** @var bool  */
    private $_withTotals = false;

    /**
     * @var null
     */
    public $sample = null;
    public $preWhere = null;
    public $limitBy = null;

    /**
     * Creates a DB command that can be used to execute this query.
     * @param \kak\clickhouse\Connection $db the database connection used to generate the SQL statement.
     * If this parameter is not given, the `db` application component will be used.
     * @return \kak\clickhouse\Command the created DB command instance.
     */
    public function createCommand($db = null)
    {
        if ($db === null) {
            $db = \Yii::$app->get('clickhouse');
        }
        list ($sql, $params) = $db->getQueryBuilder()->build($this);


        $this->_command = $db->createCommand($sql, $params);
        return $this->_command;
    }

    /**
     * set section query SAMPLE
     * @param $n float|int  set value 0.1 .. 1 percent or int 1 .. 1m value
     * @return $this the query object itself
     */
    public function sample($n)
    {
        $this->sample = $n;
        return $this;
    }


    /**
     * Sets the PREWHERE part of the query.
     *
     * The method requires a `$condition` parameter, and optionally a `$params` parameter
     * specifying the values to be bound to the query.
     *
     * The `$condition` parameter should be either a string (e.g. `'id=1'`) or an array.
     *
     * @inheritdoc
     *
     * @param string|array|Expression $condition the conditions that should be put in the WHERE part.
     * @param array $params the parameters (name => value) to be bound to the query.
     * @return $this the query object itself
     *** see andWhere()
     *** see orWhere()
     */
    public function preWhere($condition, $params = [])
    {
        $this->preWhere = $condition;
        $this->addParams($params);
        return $this;
    }


    /**
     * Adds an additional PREWHERE condition to the existing one.
     * The new condition and the existing one will be joined using the 'AND' operator.
     * @param string|array|Expression $condition the new WHERE condition. Please refer to [[where()]]
     * on how to specify this parameter.
     * @param array $params the parameters (name => value) to be bound to the query.
     * @return $this the query object itself
     * @see preWhere()
     * @see orPreWhere()
     */
    public function andPreWhere($condition, $params = [])
    {
        if ($this->preWhere === null) {
            $this->preWhere = $condition;
        } else {
            $this->preWhere = ['and', $this->preWhere, $condition];
        }
        $this->addParams($params);
        return $this;
    }

    /**
     * Adds an additional PREWHERE condition to the existing one.
     * The new condition and the existing one will be joined using the 'OR' operator.
     * @param string|array|Expression $condition the new WHERE condition. Please refer to [[where()]]
     * on how to specify this parameter.
     * @param array $params the parameters (name => value) to be bound to the query.
     * @return $this the query object itself
     * @see preWhere()
     * @see andPreWhere()
     */
    public function orPreWhere($condition, $params = [])
    {
        if ($this->preWhere === null) {
            $this->preWhere = $condition;
        } else {
            $this->preWhere = ['or', $this->preWhere, $condition];
        }
        $this->addParams($params);
        return $this;
    }

    /**
     * @return $this
     */
    public function limitBy($n, $columns)
    {
        $this->limitBy = [$n , $columns];
        return $this;
    }

    /**
     * @return $this
     */
    public function withTotals()
    {
        $this->_withTotals = true;
        return $this;
    }

    /**
     * @return bool
     */
    public function hasWithTotals()
    {
        return $this->_withTotals;
    }

    /**
     * check is first method executed
     * @throws DbException
     */
    private function ensureQueryExecuted()
    {
        if( null === $this->_command ) {
            throw new DbException('Query was not executed yet');
        }
    }

    /**
     * call method Command::{$name}
     * @param $name
     * @return mixed
     */
    private function callSpecialCommand($name)
    {
        $this->ensureQueryExecuted();
        return $this->_command->{$name}();
    }


    public function __call($name, $params)
    {
        $methods = [ 'getmeta', 'getdata', 'getextremes', 'gettotals', 'getcountall', 'getrows'];
        if (in_array(strtolower( $name), $methods)) {
            return $this->callSpecialCommand( $name );
        }
        return parent::__call($name, $params);
    }

    /**
     * reset command
     */
    public function __clone()
    {
        $this->_command = null;
        parent::__clone();
    }


}