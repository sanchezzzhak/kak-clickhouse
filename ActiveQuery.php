<?php
namespace kak\clickhouse;
use yii\db\ActiveQueryInterface;

class ActiveQuery extends \yii\db\ActiveQuery implements ActiveQueryInterface
{
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
        return parent::count($q,$db);
    }


}