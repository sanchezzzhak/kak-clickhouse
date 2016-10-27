<?php
namespace kak\clickhouse\gii\model;
/**
 * Created by PhpStorm.
 * User: PHPdev
 * Date: 27.10.2016
 * Time: 14:58
 */

use kak\clickhouse\Schema;
use Yii;
use yii\base\NotSupportedException;
use yii\helpers\ArrayHelper;
use kak\clickhouse\ActiveRecord;
use kak\clickhouse\Connection;
use yii\gii\CodeFile;
use yii\helpers\Inflector;

class Generator extends \yii\gii\Generator
{

    public $db = 'clickhouse';
    public $ns = 'app\models';
    public $collectionName;
    public $modelClass;
    public $generateLabelsFromComments = false;
    public $baseClass = 'kak\clickhouse\ActiveRecord';

    /**
     * @return string name of the code generator
     */
    public function getName()
    {
        return 'ClickHouse Model Generator';
    }

    /**
     * @inheritdoc
     */
    public function getDescription()
    {
        return 'This generator generates an ActiveRecord class for the specified ClickHouse tables.';
    }
    /**
     * @inheritdoc
     */
    public function rules()
    {
        return array_merge(parent::rules(), [
            [['db', 'ns', 'collectionName',  'modelClass', 'baseClass'], 'filter', 'filter' => 'trim'],
            [['ns'], 'filter', 'filter' => function($value) { return trim($value, '\\'); }],
            [['db', 'ns', 'collectionName', 'baseClass'], 'required'],
            [['db', 'modelClass'], 'match', 'pattern' => '/^\w+$/', 'message' => 'Only word characters are allowed.'],
            [['ns', 'baseClass'], 'match', 'pattern' => '/^[\w\\\\]+$/', 'message' => 'Only word characters and backslashes are allowed.'],
            [['collectionName'], 'match', 'pattern' => '/^[^$ ]+$/', 'message' => 'Collection name can not contain spaces or "$" symbols.'],
            [['db'], 'validateDb'],
            [['ns'], 'validateNamespace'],
            [['collectionName'], 'validateCollectionName'],
            [['modelClass'], 'validateModelClass', 'skipOnEmpty' => false],
            [['baseClass'], 'validateClass', 'params' => ['extends' => ActiveRecord::className()]],
            [['enableI18N'], 'boolean'],
            [['messageCategory'], 'validateMessageCategory', 'skipOnEmpty' => false],
        ]);
    }
    /**
     * @inheritdoc
     */
    public function attributeLabels()
    {
        return array_merge(parent::attributeLabels(), [
            'ns' => 'Namespace',
            'db' => 'ClickHouse Connection ID',
            'collectionName' => 'Collection Name',
            'modelClass' => 'Model Class',
            'baseClass' => 'Base Class',
        ]);
    }
    /**
     * @inheritdoc
     */
    public function hints()
    {
        return array_merge(parent::hints(), [
            'ns' => 'This is the namespace of the ActiveRecord class to be generated, e.g., <code>app\models</code>',
            'db' => 'This is the ID of the ClockHouse application component.',
            'collectionName' => 'This is the name of the ClickHouse table that the new ActiveRecord class is associated with, e.g. <code>post</code>.',
            'modelClass' => 'This is the name of the ActiveRecord class to be generated. The class name should not contain
                the namespace part as it is specified in "Namespace". You may leave this field blank - in this case class name
                will be generated automatically.',
            'baseClass' => 'This is the base class of the new ActiveRecord class. It should be a fully qualified namespaced class name.',
        ]);
    }
    /**
     * @inheritdoc
     */
    public function autoCompleteData()
    {
        $db = $this->getDbConnection();
        if ($db !== null) {
            return [
                'collectionName' => function () use ($db) {
                    $sql = 'SHOW TABLES';
                    $collections = $db->createCommand($sql)->queryAll();
                    return ArrayHelper::getColumn($collections, 'name');
                },
            ];
        }

        return [];

    }
    /**
     * @inheritdoc
     */
    public function requiredTemplates()
    {
        return ['model.php'];
    }
    /**
     * @inheritdoc
     */
    public function stickyAttributes()
    {
        return array_merge(parent::stickyAttributes(), ['ns', 'db', 'baseClass']);
    }
    /**
     * @inheritdoc
     */
    public function generate()
    {
        $files = [];
        $collectionName = $this->collectionName;

        $tableSchema = $this->getDbConnection()->getTableSchema($collectionName,true);

        $className = $this->generateClassName($this->modelClass);
        $params = [
            'collectionName' => $collectionName,
            'className' => $className,
            'tableSchema' => $tableSchema,
            'labels' => $this->generateLabels($tableSchema),
            'rules' => $this->generateRules($tableSchema),
        ];

        $files[] = new CodeFile(
            Yii::getAlias('@' . str_replace('\\', '/', $this->ns)) . '/' . $className . '.php',
            $this->render('model.php', $params)
        );
        return $files;
    }

    /**
     * Generates the attribute labels for the specified table.
     * @param \kak\clickhouse\TableSchema $table the table schema
     * @return array the generated attribute labels (name => label)
     */
    public function generateLabels($table)
    {
        $labels = [];
        foreach ($table->columns as $column) {
            if ($this->generateLabelsFromComments && !empty($column->comment)) {
                $labels[$column->name] = $column->comment;
            } elseif (!strcasecmp($column->name, 'id')) {
                $labels[$column->name] = 'ID';
            } else {
                $label = Inflector::camel2words($column->name);
                if (!empty($label) && substr_compare($label, ' id', -3, 3, true) === 0) {
                    $label = substr($label, 0, -3) . ' ID';
                }
                $labels[$column->name] = $label;
            }
        }
        return $labels;
    }

    /**
     * Generates validation rules for the specified table.
     * @param \kak\clickhouse\TableSchema $table the table schema
     * @return array the generated validation rules
     */
    public function generateRules($table)
    {
        $types = [];
        $lengths = [];
        foreach ($table->columns as $column) {
            if ($column->autoIncrement) {
                continue;
            }
            if (!$column->allowNull && $column->defaultValue === null) {
                $types['required'][] = $column->name;
            }
            switch ($column->type) {
                case Schema::TYPE_SMALLINT:
                case Schema::TYPE_INTEGER:
                case Schema::TYPE_BIGINT:
                    $types['integer'][] = $column->name;
                    break;
                case Schema::TYPE_BOOLEAN:
                    $types['boolean'][] = $column->name;
                    break;
                case Schema::TYPE_FLOAT:
                case 'double': // Schema::TYPE_DOUBLE, which is available since Yii 2.0.3
                case Schema::TYPE_DECIMAL:
                case Schema::TYPE_MONEY:
                    $types['number'][] = $column->name;
                    break;
                case Schema::TYPE_DATE:
                case Schema::TYPE_TIME:
                case Schema::TYPE_DATETIME:
                case Schema::TYPE_TIMESTAMP:
                    $types['safe'][] = $column->name;
                    break;
                default: // strings
                    if ($column->size > 0) {
                        $lengths[$column->size][] = $column->name;
                    } else {
                        $types['string'][] = $column->name;
                    }
            }
        }
        $rules = [];
        foreach ($types as $type => $columns) {
            $rules[] = "[['" . implode("', '", $columns) . "'], '$type']";
        }
        foreach ($lengths as $length => $columns) {
            $rules[] = "[['" . implode("', '", $columns) . "'], 'string', 'max' => $length]";
        }

       return $rules;
    }
    /**
     * Validates the [[db]] attribute.
     */
    public function validateDb()
    {
        if (!Yii::$app->has($this->db)) {
            $this->addError('db', 'There is no application component named "' . $this->db . '".');
        } elseif (!Yii::$app->get($this->db) instanceof Connection) {
            $this->addError('db', 'The "' . $this->db . '" application component must be a ClickHouse connection instance.');
        }
    }
    /**
     * Validates the [[ns]] attribute.
     */
    public function validateNamespace()
    {
        $this->ns = ltrim($this->ns, '\\');
        $path = Yii::getAlias('@' . str_replace('\\', '/', $this->ns), false);
        if ($path === false) {
            $this->addError('ns', 'Namespace must be associated with an existing directory.');
        }
    }
    /**
     * Validates the [[modelClass]] attribute.
     */
    public function validateModelClass()
    {
        if ($this->isReservedKeyword($this->modelClass)) {
            $this->addError('modelClass', 'Class name cannot be a reserved PHP keyword.');
        }
    }
    /**
     * Validates the [[collectionName]] attribute.
     */
    public function validateCollectionName()
    {
        if (empty($this->modelClass)) {
            $class = $this->generateClassName($this->collectionName);
            if ($this->isReservedKeyword($class)) {
                $this->addError('collectionName', "Collection '{$this->collectionName}' will generate a class which is a reserved PHP keyword.");
            }
        }
    }
    /**
     * Generates a class name from the specified collection name.
     * @param string $collectionName the collection name (which may contain schema prefix)
     * @return string the generated class name
     */
    protected function generateClassName($collectionName)
    {
        $className = preg_replace('/[^\\w]+/is', '_', $collectionName);
        return Inflector::id2camel($className, '_');
    }

    /**
     * @return Connection the DB connection as specified by [[db]].
     */
    protected function getDbConnection()
    {
        return Yii::$app->get($this->db, false);
    }
}