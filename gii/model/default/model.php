<?php
/**
 * This is the template for generating the model class of a specified collection.
 */

/* @var $this yii\web\View */
/* @var $generator \kak\clickhouse\gii\model\Generator */
/* @var $collectionName string full collection name */
/* @var $className string class name */
/* @var $labels string[] list of attribute labels (name => label) */
/* @var $rules string[] list of validation rules */

echo "<?php\n";
?>

namespace <?= $generator->ns ?>;
use Yii;

/**
* This is the model class for table "<?= $collectionName ?>".
*
<?php foreach ($tableSchema->columns as $column): ?>
* @property <?= "{$column->phpType} \${$column->name}\n" ?>
<?php endforeach; ?>
*/
class <?= $className ?> extends <?= '\\' . ltrim($generator->baseClass, '\\') . "\n" ?>
{
    /**
    * Get table name
    * @return string
    */
    public static function tableName()
    {
        return '<?= $collectionName ?>';
    }

    /**
    * @return \kak\clickhouse\Connection the ClickHouse connection used by this AR class.
    */
    public static function getDb()
    {
        return Yii::$app->get('<?= $generator->db ?>');
    }


    /**
    * @inheritdoc
    * @return Array
    */
    public function rules()
    {
        return [<?= "\n            " . implode(",\n            ", $rules) . "\n        " ?>];
    }

    /**
    * @inheritdoc
    * @return Array
    */
    public function attributeLabels()
    {
        return [
<?php foreach ($labels as $name => $label): ?>
            <?= "'$name' => " . $generator->generateString($label) . ",\n" ?>
<?php endforeach; ?>
        ];
    }
}
