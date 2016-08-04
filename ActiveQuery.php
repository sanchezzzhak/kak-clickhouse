<?php
namespace kak\clickhouse;


use yii\base\Component;
use yii\db\ActiveQueryInterface;
use yii\db\ActiveRecordInterface;
use yii\db\Connection;
use yii\db\QueryInterface;

class ActiveQuery extends \yii\db\ActiveQuery implements ActiveQueryInterface
{

    /**
     * Creates a DB command that can be used to execute this query.
     * @param Connection $db the DB connection used to create the DB command.
     * If null, the DB connection returned by [[modelClass]] will be used.
     * @return Command the created DB command instance.
     */
    public function createCommand($db = null)
    {
        /* @var $modelClass ActiveRecord */
        $modelClass = $this->modelClass;
        if ($db === null) {
            $db = $modelClass::getDb();
        }

        if ($this->sql === null) {
            list ($sql, $params) = $db->getQueryBuilder()->build($this);
        } else {
            $sql = $this->sql;
            $params = $this->params;
        }

        return $db->createCommand($sql, $params);
    }

}