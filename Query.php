<?php
/**
 * Created by PhpStorm.
 * User: kak
 * Date: 03.05.2017
 * Time: 11:53
 */
namespace kak\clickhouse;

use yii\db\Query as BaseQuery;
use yii\db\QueryTrait;

/**
 * Class Query
 * @package kak\clickhouse
 */
class Query extends BaseQuery
{

    private $_withTotals = false;
    private $_withMetaData = false;

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

        return $db->createCommand($sql, $params);
    }


    /**
     * @param null $db
     * @return array|mixed
     */
    public function one($db = null)
    {
        $fetchMode = $this->getFetchMode();
        return $this->createCommand($db)->queryOne($fetchMode);
    }


    /**
     * @param null $db
     * @return array|mixed
     */
    public function all($db = null )
    {
        $fetchMode  = $this->getFetchMode();
        return $this->createCommand($db)->queryAll($fetchMode);
    }

    /**
     * @return int|null
     */
    private function getFetchMode()
    {
        if($this->hasWithMetaData()){
            return Command::FETCH_MODE_ALL;
        }

        if($this->hasWithTotals()){
            return Command::FETCH_MODE_TOTAL;
        }

        return null;
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
     * @return $this
     */
    public function withMetaData()
    {
        $this->_withMetaData = true;
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
     * @return bool
     */
    public function hasWithMetaData()
    {
        return $this->_withMetaData;
    }

}