<?php

use kak\clickhouse\ActiveRecord;
use kak\clickhouse\Query;
use yii\db\Expression;


class ClickHouseTest extends \Codeception\Test\Unit
{
    public $appConfig = '@tests/_config/unit.php';

    /**
     * @return \kak\clickhouse\Connection
     */
    protected function getDb()
    {
        return Yii::$app->clickhouse;
    }

    private function genUuidFromClickHouseQuery() {
        $query = (new Query())->select([
            new Expression('generateUUIDv4() uuid')
        ]);
        return $query->scalar($this->getDb());
    }


    private function markTestSkipIsTableNotExist()
    {
        $schema = $this->getDb()->getTableSchema(
            TestTableModel::tableName()
        );
        if ($schema === null) {
            $this->markTestSkipped('Test table `test_stat` not exist');
        }
        $this->genUuidFromClickHouseQuery();
    }

    /**
     * @var \UnitTester
     */
    protected $tester;

    protected function _before()
    {
    }

    protected function _after()
    {
    }

    public function testTableTestStatExist()
    {
        $this->markTestSkipIsTableNotExist();

        $schema = $this->getDb()->getTableSchema(
            TestTableModel::tableName()
        );
        $check = $schema !== null && $schema->name === TestTableModel::tableName();
        $this->assertTrue($check, sprintf('not equals test table %s', $schema->name));
    }


    public function testSaveActiveRecord()
    {
        $model = new TestTableModel();
        $model->event_date =  date('Y-m-d');
        $model->time =  time();
        $model->user_id = rand(1, 10);
        $model->active = '1';
        $model->test_uint64 = '12873305439719614842';
        $model->test_int64 = '9223372036854775807';
        $model->test_ipv4 = sprintf('%d.%d.%d.%d',
            mt_rand(0, 255), mt_rand(0, 255), mt_rand(0, 255), mt_rand(0, 255)
        );
        $model->test_ipv6 = '2404:6800:4001:805::1008';
        $model->test_uuid = $this->genUuidFromClickHouseQuery();

        $this->assertTrue($model->save());

        $findModel = TestTableModel::findOne([
            'user_id' => $model->user_id,
            'time' => $model->time,
        ]);

        $this->assertTrue($findModel !== null, 'model not found');


        $this->assertEquals($findModel->event_date, $model->event_date);
        $this->assertEquals($findModel->time, $model->time);
        $this->assertEquals($findModel->user_id, $model->user_id);
        $this->assertEquals($model->test_ipv4, $model->test_ipv4);
        $this->assertEquals($model->test_ipv6, $model->test_ipv6);
        $this->assertEquals($model->test_uuid, $model->test_uuid);
    }

    public function testBachQuery()
    {
        $query = new Query();
        $batch = $query->select('*')->from(TestTableModel::tableName());
        foreach ($query->batch(100) as $rows) {

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

    public function testQuoteString()
    {
        $standard = "'test'";
        $result = $this->getDb()->quoteValue('test');
        $this->assertTrue($standard === $result, sprintf('quote string %s', $result));
    }

    public function testQuoteInteger()
    {
        $standard = 5;
        $result = $this->getDb()->quoteValue(5);
        $this->assertTrue($standard === $result, sprintf('quote integer %s', $result));
    }

    public function testQuoteFloat()
    {
        $standard = .4;
        $result = $this->getDb()->quoteValue(.4);
        $this->assertTrue($standard === $result, sprintf('quote float %s', $result));
    }

    public function testQuoteQuertyParams()
    {
        $standard = "SELECT * FROM test_stat WHERE user_id=1";
        $result = TestTableModel::find()->where(['user_id' => '1'])->createCommand()->getRawSql();
        $this->assertFalse($standard === $result, sprintf('sql quote %s', $result));
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
        $this->assertTrue($sql === $result, 'build query SAMPLE (generation sql builder) check false');

        $sql = TestTableModel::find()->sample($sample)->where(['user_id' => 1])->createCommand()->getRawSql();
        $this->assertTrue($sql === $result, 'build query SAMPLE (generation active record builder) check false');
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
        $this->assertTrue($sql === $result, 'build query SAMPLE (generation sql builder) check false');

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

        $this->assertTrue($sql === $result, 'build query PREWHERE check false');

        $copy = clone $query;
        $sql = $copy->createCommand()->getRawSql();
        $this->assertTrue($sql === $result, 'build copy query PREWHERE check false');

    }


    public function testUnionQuery()
    {
        $query = TestTableModel::find()->select(['t' => 'time']);

        $query->union(TestTableModel::find()->select(['t' => 'user_id']), true);
        $result = "SELECT time AS t FROM test_stat UNION ALL SELECT user_id AS t FROM test_stat";
        $sql = $query->createCommand($this->getDb())->getRawSql();
        $this->assertTrue($sql === $result, 'Simple union case');
    }

    public function testLimitByQuery()
    {

        $result = implode(' ', [
            'SELECT domainWithoutWWW(URL) AS domain, domainWithoutWWW(REFERRER_URL) AS referrer, device_type, count() AS cnt',
            'FROM test_hits GROUP BY domain, referrer, device_type',
            'ORDER BY cnt DESC LIMIT 5 BY domain, device_type LIMIT 100'
        ]);

        $query = new Query();
        $query->select([
            'domainWithoutWWW(URL) AS domain',
            'domainWithoutWWW(REFERRER_URL) AS referrer',
            'device_type',
            'count() cnt'
        ])
            ->from('test_hits')
            ->groupBy('domain, referrer, device_type')
            ->orderBy(['cnt' => SORT_DESC])
            ->limitBy(5, ['domain, device_type'])->limit(100);

        $sql = $query->createCommand($this->getDb())->getRawSql();

        $this->assertEquals($result, $sql, 'Simple limit by check');
    }
    
    public function testExpressionSelect()
    {
        $uiid = $this->genUuidFromClickHouseQuery();
        $pattern = '~^[0-9A-F]{8}-[0-9A-F]{4}-[4][0-9A-F]{3}-[89AB][0-9A-F]{3}-[0-9A-F]{12}$~i';
        $check = preg_match($pattern, $uiid) !== false && $uiid !== '00000000-0000-0000-0000-000000000000';
        $this->assertTrue($check, 'select expression not correct result');
    }

    public function testTypecast()
    {
        $schema = $this->getDb()->getTableSchema('test_stat');
        $this->assertEquals($schema->getColumn('user_id')->type, 'integer');
        $this->assertEquals($schema->getColumn('user_id')->dbTypecast(1), 1);
        $this->assertEquals($schema->getColumn('user_id')->dbTypecast('1'), 1);
        $this->assertEquals($schema->getColumn('user_id')->dbTypecast(null), null);
        $this->assertEquals($schema->getColumn('active')->type, 'smallint');
        $this->assertEquals($schema->getColumn('active')->dbTypecast(1), 1);
        $this->assertEquals($schema->getColumn('active')->dbTypecast('1'), 1);
        $this->assertEquals($schema->getColumn('active')->dbTypecast(null), null);
        // Clickhouse has no TRUE AND FALSE, so TRUE always transform to 1 and FALSE - to 0
        $this->assertEquals($schema->getColumn('active')->dbTypecast(false), 0);
    }
}
