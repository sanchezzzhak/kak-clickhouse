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


}
