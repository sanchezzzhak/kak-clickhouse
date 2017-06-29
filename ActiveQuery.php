<?php
namespace kak\clickhouse;
use yii\db\ActiveQueryInterface;
use yii\db\ActiveQueryTrait;
use yii\db\ActiveRelationTrait;

/**
 * Class ActiveQuery
 * @package kak\clickhouse
 */
class ActiveQuery extends Query implements ActiveQueryInterface
{
    use ActiveQueryTrait;
    use ActiveRelationTrait;

    /**
     * Constructor.
     * @param array $modelClass the model class associated with this query
     * @param array $config configurations to be applied to the newly created query object
     */
    public function __construct($modelClass, $config = [])
    {
        $this->modelClass = $modelClass;
        parent::__construct($config);
    }
    
    /**
    * Creates a DB command that can be used to execute this query.
    * @param Connection|null $db the DB connection used to create the DB command.
    * If `null`, the DB connection returned by [[modelClass]] will be used.
    * @return Command the created DB command instance.
    */
    public function createCommand($db = null)
    {
        $modelClass = $this->modelClass;
        return parent::createCommand($db ? $db : $modelClass::getDb());
    }

    /**
     * Returns the number of records.
     * @param string $q the COUNT expression. Defaults to ''. clickhouse not support
     * Make sure you properly [quote](guide:db-dao#quoting-table-and-column-names) column names in the expression.
     * @param Connection $db the database connection used to generate the SQL statement.
     * If this parameter is not given (or null), the `db` application component will be used.
     * @return integer|string number of records. The result may be a string depending on the
     * underlying database engine and to support integer values higher than a 32bit PHP integer can handle.
     */
    public function count($q = '', $db = null)
    {
        return parent::count($q, $db);
    }




}
