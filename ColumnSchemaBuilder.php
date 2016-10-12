<?php
/**
 * @author    Dmytro Karpovych
 * @copyright 2016 NRE
 */


namespace kak\clickhouse;


use yii\db\ColumnSchemaBuilder as BaseColumnSchemaBuilder;

class ColumnSchemaBuilder extends BaseColumnSchemaBuilder
{
    /**
     * @inheritdoc
     */
    public function __toString()
    {
        switch ($this->getTypeCategory()) {
            case self::CATEGORY_NUMERIC:
                $format = '{unsigned}{type}{default}';
                break;
            default:
                $format = '{type}{default}';
        }

        return $this->buildCompleteString($format);
    }

    /**
     * @inheritdoc
     */
    protected function buildUnsignedString()
    {
        return $this->isUnsigned ? 'U' : '';
    }
}