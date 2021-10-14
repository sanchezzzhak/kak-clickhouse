<?php
namespace kak\clickhouse\tests\unit\models;

use kak\clickhouse\ActiveRecord;

/**
 * Class TestTableModel
 * @property string $event_date
 * @property int $time
 * @property int $user_id
 * @property int $active
 * @property int|string $test_uint64
 * @property int|string $test_int64
 * @property string $test_ipv4
 * @property string $test_ipv6
 * @property string $test_uuid
 * @property string $test_enum
 * @property array $test_array
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