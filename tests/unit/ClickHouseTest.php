<?php
include_once "models\TestTableModel.php";

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


    public function testPing(){

      //  $this->assertTrue($this->getDb()->ping(), 'call ping success result');
    }

    public function testActiveRecordQueryBuild()
    {
        $query = tests\models\TestTableModel::find();

        var_dump($query->where(['user_id' => 1])->createCommand()->getRawSql());

    }


}