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
    use QueryTrait;

    public $withTotals = false;

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
     * @param bool $set
     * @return $this
     */
    public function withTotals($set = true)
    {
        $this->withTotals = $set;
        return $this;
    }


    public function one($db = null, $fetchMode = null )
    {
        return $this->createCommand($db)->queryOne($fetchMode);
    }

    public function all($db = null, $fetchMode = null )
    {
        return $this->createCommand($db)->queryAll($fetchMode);
    }


}