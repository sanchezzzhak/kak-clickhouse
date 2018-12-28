<?php


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

    private function markTestSkipIsTableNotExist()
    {
        $schema =  $this->getDb()->getTableSchema(
            TestTableModel::tableName()
        );
        if($schema !== null){
            $this->markTestSkipped('Test table `test_stat` not exist');
        }
    }

    /**
     * @var \UnitTester
     */
    protected $tester;

    protected function _before()
    {}

    protected function _after()
    {}

    public function testTableTestStatExist()
    {
        $schema =  $this->getDb()->getTableSchema(
            TestTableModel::tableName()
        );
        $this->assertTrue($schema === null && $schema->name === TestTableModel::tableName(), 'not equals test table' . $schema->name);
    }

    public function testSaveActiveRecord()
    {
        $model = new TestTableModel();
        $model->event_date = date('Y-m-d');
        $model->time = time();
        $model->user_id = rand(1,10);
        $model->active = '1';

        $this->assertTrue( $model->save());
        $findModel = TestTableModel::findOne([
            'user_id' => $model->user_id,
            'time' => $model->time
        ]);

        $this->assertTrue( $findModel !== null, 'find model not found');
    }

    public function testBachQuery()
    {
        $query = new \kak\clickhouse\Query();
        $batch = $query->select('*')
            ->from(TestTableModel::tableName());

        foreach ($query->batch(100) as $rows) {
//            var_dump($rows);
//            echo "======\n";
        }
    }


    public function testQuoteValues()
    {
        $result = "'test'";
        $this->assertTrue($this->getDb()->quoteValue('test') === $result, 'quote string ' . $result);
        $result = $this->getDb()->quoteValue(5);
        $this->assertTrue(5 === $result, 'no quote integer ' . $result);
        $result = $this->getDb()->quoteValue(.4);
        $this->assertTrue($result === .4, 'no quote float ' . $result);

        $result = "SELECT * FROM test_stat WHERE user_id=1";
        $sql = TestTableModel::find()->where(['user_id' => '1'])->createCommand()->getRawSql();
        $this->assertFalse($result === $sql, 'sql quote error' . $sql);
    }


    public function testSampleSectionQuery()
    {
        $table = TestTableModel::tableName();
        $sample = 0.5;
        $query = (new \kak\clickhouse\Query())->select('*');
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

        $query = (new \kak\clickhouse\Query())->select('*');
        $query->from($table);
        $query->sample('1/10 OFFSET 2/10');
        $query->where(['user_id' => 1]);
        $sql = $query->createCommand()->getRawSql();
        $this->assertTrue($sql === $result, 'build query SAMPLE (generation sql builder) check false');

    }


    public function testPreWhereSectionQuery()
    {
        $table = TestTableModel::tableName();
        $query = (new \kak\clickhouse\Query())->select('*');
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

        $result = 'SELECT domainWithoutWWW(URL) AS domain, domainWithoutWWW(REFERRER_URL) AS referrer, device_type, count() cnt FROM hits GROUP BY domain, referrer, device_type ORDER BY cnt DESC LIMIT 5 BY domain, device_type LIMIT 100';

        $query = new \kak\clickhouse\Query();
        $query->select([
            'domainWithoutWWW(URL) AS domain',
            'domainWithoutWWW(REFERRER_URL) AS referrer',
            'device_type',
            'count() cnt'
        ])
            ->from('hits')
            ->groupBy('domain, referrer, device_type')
            ->orderBy(['cnt' => SORT_DESC])
            ->limitBy(5, ['domain, device_type'])->limit(100);

        $sql = $query->createCommand($this->getDb())->getRawSql();

        $this->assertEquals($result, $sql, 'Simple limit by check');
    }

    public function testTypecast()
    {
        $this->assertEquals($this->getDb()->getTableSchema('test_stat')->getColumn('user_id')->type, 'integer');
        $this->assertEquals($this->getDb()->getTableSchema('test_stat')->getColumn('user_id')->dbTypecast(1), 1);
        $this->assertEquals($this->getDb()->getTableSchema('test_stat')->getColumn('user_id')->dbTypecast('1'), 1);
        $this->assertEquals($this->getDb()->getTableSchema('test_stat')->getColumn('user_id')->dbTypecast(null), null);

        $this->assertEquals($this->getDb()->getTableSchema('test_stat')->getColumn('active')->type, 'smallint');
        $this->assertEquals($this->getDb()->getTableSchema('test_stat')->getColumn('active')->dbTypecast(1), 1);
        $this->assertEquals($this->getDb()->getTableSchema('test_stat')->getColumn('active')->dbTypecast('1'), 1);
        $this->assertEquals($this->getDb()->getTableSchema('test_stat')->getColumn('active')->dbTypecast(null), null);
        // Clickhouse has no TRUE AND FALSE, so TRUE always transform to 1 and FALSE - to 0
        $this->assertEquals($this->getDb()->getTableSchema('test_stat')->getColumn('active')->dbTypecast(false), 0);
    }
}
