<?php




class ClickHouseTest extends \yii\codeception\TestCase
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
        $this->assertTrue($this->getDb()->quoteValue('test') === $result,'quote string ' . $result);
        $result = $this->getDb()->quoteValue(5);
        $this->assertTrue(5 === $result ,'no quote integer ' . $result);
        $result = $this->getDb()->quoteValue(.4);
        $this->assertTrue($result === .4 ,'no quote float ' . $result);

        $result = "SELECT * FROM test_stat WHERE user_id=1";
        $sql = TestTableModel::find()->where(['user_id' => '1'])->createCommand()->getRawSql();
        $this->assertFalse($result === $sql ,'sql quote error' . $sql);
    }


    public function testSampleSectionQuery()
    {
        $table = TestTableModel::tableName();
        $sample = 0.5;
        $query = ( new \kak\clickhouse\Query())->select('*');
        $query->from($table);
        $query->sample($sample);
        $query->where(['user_id' => 1 ]);

        $result = "SELECT * FROM test_stat  SAMPLE 0.5 WHERE user_id=1";
        $sql = $query->createCommand()->getRawSql();
        $this->assertTrue($sql === $result ,'build query SAMPLE (generation sql builder) check false');

        $sql = TestTableModel::find()->sample($sample)->where(['user_id' => 1])->createCommand()->getRawSql();
        $this->assertTrue($sql === $result ,'build query SAMPLE (generation active record builder) check false');

    }


    public function testPreWhereSectionQuery()
    {
        $table = TestTableModel::tableName();
        $query = ( new \kak\clickhouse\Query())->select('*');
        $query->from($table);
        $query->preWhere(['user_id' => 2]);
        $query->orPreWhere('user_id=3');
        $query->where('user_id > 1');

        $sql = $query->createCommand()->getRawSql();
        $result = "SELECT * FROM test_stat PREWHERE (user_id=2) OR (user_id=3) WHERE user_id > 1";

        $this->assertTrue($sql === $result ,'build query PREWHERE check false');

        $copy = clone $query;
        $sql = $copy->createCommand()->getRawSql();
        $this->assertTrue($sql === $result ,'build copy query PREWHERE check false');

    }




}