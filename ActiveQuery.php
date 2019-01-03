<?php
namespace kak\clickhouse;
use yii\base\InvalidConfigException;
use yii\db\ActiveQueryInterface;
use yii\db\ActiveQueryTrait;
use yii\db\ActiveRecordInterface;
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

    /**
     * Executes query and returns all results as an array.
     * @param Connection $db the DB connection used to create the DB command.
     * If null, the DB connection returned by [[modelClass]] will be used.
     * @return array|ActiveRecord[] the query results. If the query results in nothing, an empty array will be returned.
     */
    public function all($db = null)
    {
        return parent::all($db);
    }

    /**
     * @param null $db
     * @return ActiveRecord|null
     * @throws InvalidConfigException
     */
    public function one($db = null)
    {
        $row = parent::one($db);
        if ($row !== null) {
            $models = $this->populate([$row]);
            return reset($models) ?: null;
        }

        return null;
    }

    /**
     * {@inheritdoc}
     * @throws InvalidConfigException
     */
    public function populate($rows)
    {
        if (empty($rows)) {
            return [];
        }

        $models = $this->createModels($rows);
        if (!empty($this->join) && $this->indexBy === null) {
            $models = $this->removeDuplicatedModels($models);
        }
        if (!empty($this->with)) {
            $this->findWith($this->with, $models);
        }

        if ($this->inverseOf !== null) {
            $this->addInverseRelations($models);
        }

        if (!$this->asArray) {
            foreach ($models as $model) {
                $model->afterFind();
            }
        }

        return parent::populate($models);
    }

    /**
     * Removes duplicated models by checking their primary key values.
     * This method is mainly called when a join query is performed, which may cause duplicated rows being returned.
     * @param array $models the models to be checked
     * @throws InvalidConfigException if model primary key is empty
     * @return array the distinctive models
     */
    private function removeDuplicatedModels($models)
    {
        $hash = [];
        /* @var $class ActiveRecord */
        $class = $this->modelClass;
        $pks = $class::primaryKey();

        if (count($pks) > 1) {
            // composite primary key
            foreach ($models as $i => $model) {
                $key = [];
                foreach ($pks as $pk) {
                    if (!isset($model[$pk])) {
                        // do not continue if the primary key is not part of the result set
                        break 2;
                    }
                    $key[] = $model[$pk];
                }
                $key = serialize($key);
                if (isset($hash[$key])) {
                    unset($models[$i]);
                } else {
                    $hash[$key] = true;
                }
            }
        } elseif (empty($pks)) {
            throw new InvalidConfigException("Primary key of '{$class}' can not be empty.");
        } else {
            // single column primary key
            $pk = reset($pks);
            foreach ($models as $i => $model) {
                if (!isset($model[$pk])) {
                    // do not continue if the primary key is not part of the result set
                    break;
                }
                $key = $model[$pk];
                if (isset($hash[$key])) {
                    unset($models[$i]);
                } elseif ($key !== null) {
                    $hash[$key] = true;
                }
            }
        }

        return array_values($models);
    }

    /**
     * If applicable, populate the query's primary model into the related records' inverse relationship.
     * @param array $result the array of related records as generated by [[populate()]]
     * @since 2.0.9
     */
    private function addInverseRelations(&$result)
    {
        if ($this->inverseOf === null) {
            return;
        }

        foreach ($result as $i => $relatedModel) {
            if ($relatedModel instanceof ActiveRecordInterface) {
                if (!isset($inverseRelation)) {
                    $inverseRelation = $relatedModel->getRelation($this->inverseOf);
                }
                $relatedModel->populateRelation($this->inverseOf, $inverseRelation->multiple ? [$this->primaryModel] : $this->primaryModel);
            } else {
                if (!isset($inverseRelation)) {
                    /* @var $modelClass ActiveRecordInterface */
                    $modelClass = $this->modelClass;
                    $inverseRelation = $modelClass::instance()->getRelation($this->inverseOf);
                }
                $result[$i][$this->inverseOf] = $inverseRelation->multiple ? [$this->primaryModel] : $this->primaryModel;
            }
        }
    }
}
