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
     * @param bool $upsert - is insert or update
     * @return array|bool|float|int|Expression|mixed|resource|string|ExpressionInterface|PdoValue|Query|null
     */
    public function dbTypecast($value, $upsert = false)
    {
        $dbType = $this->dbType;

        $intPattern = '~^[-]?\d+~';
        $floatPattern = '~^[-]?\d+(?:\.\d+)~';

        // insert array
        if (preg_match('~^Array\(~i', $dbType) && $upsert) {
            return new Expression(json_encode($value));
        }

        if (preg_match('~^(?:Nullable[(])?U?Int(64|256)~i', $dbType)) {
            if($value === null && $upsert) {
                return $this->defaultValue;
            }
            if ($value !== null && !preg_match($intPattern, $value)) {
                throw new Exception('Int value format error');
            }
            return new Expression($value);
        }

        if (preg_match('~^(?:Nullable[(])?U?Float(64|32)~i', $dbType)) {
            if($value === null && $upsert) {
                return $this->defaultValue;
            }
            $value = StringHelper::floatToString($value);
            if (!preg_match($floatPattern, $value)) {
                throw new Exception('Float value format error');
            }
            return new Expression($value);
        }

        if (preg_match('~^(?:Nullable[(])?U?Int~i', $dbType)) {
            if($value === null && $upsert) {
                return $this->defaultValue;
            }
            return (int)$value;
        }

        return $value;
    }

}
