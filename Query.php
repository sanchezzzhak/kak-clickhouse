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

        return $this->_command = $db->createCommand($sql, $params);
    }

    /**
     * @param null $db
     * @return array|mixed
     */
    public function one($db = null)
    {
        return $this->createCommand($db)->queryOne();
    }

    /**
     * @param null $db
     * @return array|mixed
     */
    public function all($db = null )
    {
        return $this->createCommand($db)->queryAll();
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