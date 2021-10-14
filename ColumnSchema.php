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

    public function dbTypecast($value, $upsert = false)
    {
        $dbType = $this->dbType;

        // insert array
        if(preg_match('~^Array\(~i', $dbType) && $upsert) {
            return new Expression(json_encode($value));
        }

        if (preg_match('~U?Int(64|256)~i', $dbType) ) {
            return new Expression($value ?? $this->defaultValue);
        }

        if (preg_match('~U?Float(64)~i', $dbType) ) {
            return new Expression($value ?? $this->defaultValue);
        }

        if (preg_match('~U?Int~i', $dbType)) {
            return $value ?? $this->defaultValue;
        }

        return $value;
    }

}
