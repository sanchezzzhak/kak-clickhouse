<?php

namespace kak\clickhouse\tests\unit;

use Codeception\Test\Unit;
use Exception;
use kak\clickhouse\tests\unit\models\TestTableModel;

use kak\clickhouse\{ActiveRecord, Command, Query, Expression};


use Yii;

/**
 * Class ClickHouseTest
 * @package kak\clickhouse\tests\unit
 */
class ClickHouseTest extends Unit
{
    public $appConfig = '@tests/_config/unit.php';

    /**
     * @var \UnitTester
     */
    protected $tester;

    /**
     * @return \kak\clickhouse\Connection
     */
    protected static function getDb()
    {
        return Yii::$app->clickhouse;
    }

    public function testCreateTableIsNotExist()
    {
        $db = self::getDb();
        $db->createCommand('
            CREATE TABLE IF NOT EXISTS `test_stat` (
                `event_date` Date,
                `time` Int32,
                `user_id` Int32,
                `active` Nullable(Int8),
                `test_enum` Nullable(Enum8(\'hello\' =1, \'world\' =2)),
                `test_uint64` UInt64,
                `test_int64` Int64,
                `test_ipv4` IPv4,
                `test_ipv6` IPv6,
                `test_uuid` UUID,
                `test_array` Array(Array(Array(Nullable(Int32))))
            ) ENGINE = MergeTree(event_date, (event_date, user_id), 8192);')
            ->execute();
    }

    public function testInsertRow()
    {
        $response = self::getDb()->createCommand()
            ->insert('test_stat', [
            'event_date' => date('Y-m-d'),
            'user_id' => 1,
            'test_array' => [
                [
                    [12, 13, 0, 1], [null, 11], [213]
                ], [
                    [1], [12, 32]
                ]
            ]
        ])->execute();

        $this->assertTrue(true, $response->isOk);
    }

    public function testTableTestStatExist()
    {
        $schema = self::getDb()->getTableSchema(
            TestTableModel::tableName()
        );

        $this->assertTrue(
            $schema !== null && $schema->name === TestTableModel::tableName(),
            'not equals test table' . $schema->name
        );
    }

    public function testSaveActiveRecord()
    {
        $model = new TestTableModel();
        $model->event_date = date('Y-m-d');
        $model->time = time();
        $model->user_id = 5;
        $model->active = '1';
        $model->test_enum = 'world';
        $model->test_uint64 = '12873305439719614842';
        $model->test_int64 = '9223372036854775807';
        $model->test_ipv4 = sprintf('%d.%d.%d.%d',
            mt_rand(0, 255), mt_rand(0, 255), mt_rand(0, 255), mt_rand(0, 255)
        );
        $model->test_ipv6 = '2404:6800:4001:805::1008';
        $model->test_uuid = (new Query())->select([
            new Expression('generateUUIDv4() uuid')
        ])->scalar(self::getDb());

        $model->test_array = [[[1, 2, 50]]];

        $this->assertTrue($model->save());

        $findModel = TestTableModel::findOne([
            'user_id' => $model->user_id,
            'time' => $model->time,
            'test_uuid' => $model->test_uuid
        ]);

        $this->assertNotNull($findModel, 'find model not found');
        $this->assertEquals($findModel->event_date, $model->event_date);
        $this->assertEquals($findModel->time, $model->time);
        $this->assertEquals($findModel->user_id, $model->user_id);

        $this->assertEquals($findModel->test_ipv4, $model->test_ipv4);
        $this->assertEquals($findModel->test_ipv6, $model->test_ipv6);
        $this->assertEquals($findModel->test_uuid, $model->test_uuid);
        $this->assertEquals($findModel->test_array, $model->test_array);
    }

    public function testBachQuery()
    {
        $query = new Query();
        $batch = $query->select('*')
            ->from(TestTableModel::tableName());

        foreach ($query->batch(100) as $rows) {
//            var_dump($rows);
//            echo "======\n";
        }
    }

    public function testFindModelAR()
    {
        $model = TestTableModel::find()->one();
        $this->assertTrue($model instanceof ActiveRecord);
    }

    public function testCountQuery()
    {
        $query = new Query();
        $cnt = $query->from(TestTableModel::tableName())
            ->count();

        $this->assertTrue($cnt > 0);
    }

    public function testQuoteValues()
    {
        $result = self::getDb()->quoteValue('test');
        $this->assertEquals($result, "'test'");

        $result = self::getDb()->quoteValue("wait's");
        $this->assertEquals($result, "'wait\'s'");

        $result = self::getDb()->quoteValue(5);
        $this->assertEquals($result, 5);

        $result = self::getDb()->quoteValue(.4);
        $this->assertEquals($result, .4);

        $result = TestTableModel::find()
            ->where(['user_id' => 1])
            ->createCommand()
            ->getRawSql();

        $this->assertEquals($result, "SELECT * FROM test_stat WHERE user_id=1", 'sql quote error');

    }

    public function testSampleSectionQuery()
    {
        $table = TestTableModel::tableName();
        $sample = 0.5;
        $query = (new Query())->select('*');
        $query->from($table);
        $query->sample($sample);
        $query->where(['user_id' => 1]);

        $result = "SELECT * FROM test_stat  SAMPLE 0.5 WHERE user_id=1";
        $sql = $query->createCommand()->getRawSql();
        $this->assertSame($sql, $result, 'build query SAMPLE (generation sql builder) check false');

        $sql = TestTableModel::find()->sample($sample)->where(['user_id' => 1])->createCommand()->getRawSql();
        $this->assertSame($sql, $result, 'build query SAMPLE (generation active record builder) check false');

    }


    public function testSampleOffsetSectionQuery()
    {
        $table = TestTableModel::tableName();
        $result = "SELECT * FROM test_stat  SAMPLE 1/10 OFFSET 2/10 WHERE user_id=1";
        $query = (new Query())->select('*');
        $query->from($table);
        $query->sample('1/10 OFFSET 2/10');
        $query->where(['user_id' => 1]);
        $sql = $query->createCommand()->getRawSql();
        $this->assertSame($sql, $result, 'build query SAMPLE (generation sql builder) check false');

    }

    public function testPreWhereSectionQuery()
    {
        $table = TestTableModel::tableName();
        $query = (new Query())->select('*');
        $query->from($table);
        $query->preWhere(['user_id' => 2]);
        $query->orPreWhere('user_id=3');
        $query->where('user_id > 1');

        $sql = $query->createCommand()->getRawSql();
        $result = "SELECT * FROM test_stat PREWHERE (user_id=2) OR (user_id=3) WHERE user_id > 1";

        $this->assertSame($sql, $result, 'build query PREWHERE check false');

        $copy = clone $query;
        $sql = $copy->createCommand()->getRawSql();
        $this->assertSame($sql, $result, 'build copy query PREWHERE check false');

    }

    public function testUnionQuery()
    {
        $query = TestTableModel::find()->select(['t' => 'time']);
        $query->union(TestTableModel::find()->select(['t' => 'user_id']), true);
        $result = "SELECT time AS t FROM test_stat UNION ALL SELECT user_id AS t FROM test_stat";
        $sql = $query->createCommand(self::getDb())->getRawSql();
        $this->assertSame($sql, $result, 'Simple union case');
    }

    public function testWithRollupQuery()
    {
        $command = (new Query())
            ->select(['count() as cnt', 'event_date'])
            ->from(TestTableModel::tableName())
            ->groupBy(['event_date'])
            ->limit(1)
            ->withRollup();

        $result = $command->all();

        $this->assertEquals(count($result), 1);

        $sql = $command->createCommand()->getRawSql();
        $actual = 'SELECT count() AS cnt, event_date FROM test_stat GROUP BY event_date WITH ROLLUP LIMIT 1';
        $this->assertEquals($sql, $actual);
    }

    public function testWithQuery()
    {
        $db = self::getDb();

        $date = date('Y-m-d');
        $command = (new Query())
            ->withQuery($db->quoteValue($date), 'a1')
            ->select(['count() as cnt', 'event_date'])
            ->from(TestTableModel::tableName())
            ->where('event_date = a1')
            ->groupBy(['event_date'])
            ->limit(1);

        $sql = $command->createCommand()->getRawSql();
        $actual = "WITH '$date' AS a1 SELECT count() AS cnt, event_date FROM test_stat WHERE event_date = a1 GROUP BY event_date LIMIT 1";
        $this->assertEquals($sql, $actual);
    }

    public function testWithCubeQuery()
    {
        $command = (new Query())
            ->select(['count() as cnt', 'event_date'])
            ->from(TestTableModel::tableName())
            ->groupBy(['event_date'])
            ->limit(1)
            ->withCube();

        $result = $command->all();

        $this->assertCount(1, $result);

        $sql = $command->createCommand()->getRawSql();
        $actual = 'SELECT count() AS cnt, event_date FROM test_stat GROUP BY event_date WITH CUBE LIMIT 1';
        $this->assertEquals($sql, $actual);
    }

    public function testWithTotalsQuery()
    {
        $command = (new Query())
            ->select(['count()', 'user_id'])
            ->from(TestTableModel::tableName())
            ->groupBy(['user_id'])
            ->withTotals()
            ->limit(1);

        $result = $command->all(self::getDb());

        $this->assertTrue($command->getCountAll() > 1);
        $this->assertCount(1, $result);

        $sql = $command->createCommand()->getRawSql();
        $actual = 'SELECT count(), user_id FROM test_stat GROUP BY user_id WITH TOTALS LIMIT 1';
        $this->assertEquals($sql, $actual);


        $command = self::getDb()->createCommand('select user_id from test_stat group by user_id limit 1');
        $result = $command->queryAll();
        $this->assertTrue($command->getCountAll() > 1);
        $this->assertCount(1, $result);

    }

    public function testQuoteString()
    {
        $standard = "'test'";
        $result = self::getDb()->quoteValue('test');
        $this->assertSame($standard, $result, sprintf('quote string %s', $result));
    }

    public function testQuoteInteger()
    {
        $standard = 5;
        $result = self::getDb()->quoteValue($standard);
        $this->assertSame($standard, $result, sprintf('quote integer %s', $result));
    }

    public function testQuoteFloat()
    {
        $standard = .4;
        $result = self::getDb()->quoteValue($standard);
        $this->assertSame($standard, $result, sprintf('quote float %s', $result));
    }

    public function testQuoteQuoteParams()
    {
        $standard = "SELECT * FROM test_stat WHERE user_id='1'";
        $result = TestTableModel::find()->where(['user_id' => '1'])->createCommand()->getRawSql();
        $this->assertSame($standard, $result, sprintf('sql quote %s', $result));
    }

    public function testExpressionCast()
    {
        $ips = [
            '255.146.176.212' => ip2long('255.146.176.212'),
            '19.203.21.194' => ip2long('19.203.21.194'),
        ];
        foreach ($ips as $ipStr => $ipInt) {
            $result = (new Query())->select([
                'alias_name' => Expression::cast($ipInt, 'IPv4')
            ])->scalar();

            $this->assertEquals($ipStr, $result, sprintf('cast ip "%s" int to str "%s"', $ipInt, $ipStr));
        }

        $sql = (new Query())->select([
            'alias_name' => Expression::cast(
                ip2long('112.246.76.178'),
                'IPv4'
            )
        ])->createCommand()
            ->getRawSql();

        $this->assertEquals(
            $sql,
            "SELECT CAST(1895189682 AS IPv4) AS alias_name"
        );
    }

    public function testTypecast()
    {
//        $this->assertEquals('integer', self::getDb()->getTableSchema('test_stat')->getColumn('user_id')->type);
//        $this->assertEquals(1, self::getDb()->getTableSchema('test_stat')->getColumn('user_id')->dbTypecast(1));
//        $this->assertEquals(1, self::getDb()->getTableSchema('test_stat')->getColumn('user_id')->dbTypecast('1'));
//        $this->assertEquals(null, self::getDb()->getTableSchema('test_stat')->getColumn('user_id')->dbTypecast(null));
//
//        $this->assertEquals('smallint', self::getDb()->getTableSchema('test_stat')->getColumn('active')->type);
//        $this->assertEquals(1, self::getDb()->getTableSchema('test_stat')->getColumn('active')->dbTypecast(1));
//        $this->assertEquals(1, self::getDb()->getTableSchema('test_stat')->getColumn('active')->dbTypecast('1'));
//        $this->assertEquals(null, self::getDb()->getTableSchema('test_stat')->getColumn('active')->dbTypecast(null));
//        // Clickhouse has no TRUE AND FALSE, so TRUE always transform to 1 and FALSE - to 0
//        $this->assertEquals(0, self::getDb()->getTableSchema('test_stat')->getColumn('active')->dbTypecast(false));
    }

    public function testDropTableTable()
    {
        $table = TestTableModel::tableName();
        $schema = self::getDb()->getTableSchema($table);
        if ($schema !== null) {
            self::getDb()->createCommand()
                ->dropTable($table)
                ->execute();

            $this->assertNull(self::getDb()->getTableSchema($table));
            return;
        }
        $this->markTestSkipped('table not found');
    }

}
