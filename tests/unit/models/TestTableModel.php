<?php
namespace  tests\models;

class TestTableModel extends \kak\clickhouse\ActiveRecord
{
    public static function tableName()
    {
        return 'test_stat';
    }

    /**
     * @return \kak\clickhouse\Connection;
     */
    public static function getDb()
    {
        return \Yii::$app->clickhouse;
    }


}