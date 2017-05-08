<?php
/**
 * Created by PhpStorm.
 * User: kak
 * Date: 08.05.2017
 * Time: 16:45
 */

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