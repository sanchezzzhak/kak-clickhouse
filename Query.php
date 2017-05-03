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

    public $withTotals = false;
    public $withMetaData = false;

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
     * @return $this
     */
    public function withTotals()
    {
        $this->withTotals = true;
        return $this;
    }

    /**
     * @return $this
     */
    public function withMetaData()
    {
        $this->withMetaData = true;
        return $this;
    }


    public function one($db = null)
    {
        $fetchMode  = $this->getFetchMode();
        return $this->createCommand($db)->queryOne($fetchMode);
    }


    public function all($db = null )
    {
        $fetchMode  = $this->getFetchMode();
        return $this->createCommand($db)->queryAll($fetchMode);
    }

    private function getFetchMode()
    {
        if($this->withMetaData){
            return Command::FETCH_MODE_ALL;
        }

        if($this->withTotals){
            return Command::FETCH_MODE_TOTAL;
        }

        return null;
    }

}