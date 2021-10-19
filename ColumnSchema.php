<?php
/**
 * @author    Dmytro Karpovych
 * @copyright 2016 NRE
 */


namespace kak\clickhouse;

use yii\db\ColumnSchema as BaseColumnSchema;
use yii\db\Exception;
use yii\db\ExpressionInterface;
use yii\db\PdoValue;
use yii\helpers\StringHelper;

/**
 * Class ColumnSchema
 * @package kak\clickhouse
 */
class ColumnSchema extends BaseColumnSchema
{
    /**
     * @inheritdoc
     * @param mixed $value
     * @return mixed
     */
    protected function typecast($value)
    {
        return $value;
    }

    /**
     * cast value to db format
     *
     * @param mixed $value - value
     * @return array|bool|float|int|Expression|mixed|resource|string|ExpressionInterface|PdoValue|Query|null
     */
    public function dbTypecastInsert($value)
    {
        $dbType = $this->dbType;

        $intPattern = '~^[-]?\d+~';
        $floatPattern = '~^[-]?\d+(?:\.\d+)~';


        if($value === null) {
            if($this->defaultValue !== null) {
                return $this->defaultValue;
            }
            if ($this->allowNull) {
                return null;
            }
        }

        // insert array
        if (preg_match('~^Array\(~i', $dbType)) {
            return new Expression(json_encode($value));
        }

        if (preg_match('~^(?:Nullable[(])?U?Int(64|256)~i', $dbType)) {
            if ($value !== null && !preg_match($intPattern, $value)) {
                throw new Exception('Int value format error');
            }
            return new Expression($value);
        }

        if (preg_match('~^(?:Nullable[(])?U?Float(64|32)~i', $dbType)) {
            $value = StringHelper::floatToString($value);
            if (!preg_match($floatPattern, $value)) {
                throw new Exception('Float value format error');
            }
            return new Expression($value);
        }

        if (preg_match('~^(?:Nullable[(])?U?Int~i', $dbType)) {
            return (int)$value;
        }

        return $value;
    }

}
