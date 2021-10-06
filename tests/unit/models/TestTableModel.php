<?php
namespace kak\clickhouse\tests\unit\models;

use kak\clickhouse\ActiveRecord;

/**
 * Class TestTableModel
 * @property string $event_date
 * @property int $time
 * @property int $user_id
 * @property int $active
 *
 */
class TestTableModel extends ActiveRecord
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