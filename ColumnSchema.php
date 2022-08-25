<?php
/**
 * @author    Dmytro Karpovych
 * @copyright 2016 NRE
 */


namespace kak\clickhouse;


use yii\db\ColumnSchema as BaseColumnSchema;
use yii\db\ExpressionInterface;
use yii\db\PdoValue;
use yii\helpers\StringHelper;

class ColumnSchema extends BaseColumnSchema
{
    /**
     * @inheritdoc
     * @param mixed $value
     * @return bool|float|int|PdoValue|mixed|null|string
     */
    protected function typecast($value)
    {
        if ($value === ''
            && !in_array(
                $this->type,
                [
                    Schema::TYPE_TEXT,
                    Schema::TYPE_STRING,
                    Schema::TYPE_BINARY,
                    Schema::TYPE_CHAR
                ],
                true)
        ) {
            return null;
        }

        if ($value === null
            || gettype($value) === $this->phpType
            || $value instanceof ExpressionInterface
            || $value instanceof Query
        ) {
            return $value;
        }

        if (is_array($value)
            && count($value) === 2
            && isset($value[1])
            && in_array($value[1], $this->getPdoParamTypes(), true)
        ) {
            return new PdoValue($value[0], $value[1]);
        }

        switch ($this->phpType) {
            case 'resource':
            case 'string':
                if (is_resource($value) || is_array($value)) {
                    return $value;
                }
                if (is_float($value)) {
                    // ensure type cast always has . as decimal separator in all locales
                    return StringHelper::floatToString($value);
                }
                return (string) $value;
            case 'integer':
                return (int) $value;
            case 'boolean':
                // treating a 0 bit value as false too
                // https://github.com/yiisoft/yii2/issues/9006
                return (bool) $value && $value !== "\0";
            case 'double':
                return (float) $value;
        }

        return $value;
    }
}
