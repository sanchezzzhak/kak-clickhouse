<?php


namespace kak\clickhouse;

/**
 * Class Expression
 * @package kak\clickhouse
 */
class Expression extends \yii\db\Expression
{
    /**
     * Create expression CAST
     * @uses
     * ```
     * $query = (new Query())->select([
     *    'alias_name' => Expression::cast('3228622519', 'IPv4)
     * ])->scalar()
     *
     * ```
     *
     * @param string $value
     * @param string $type
     * @return Expression
     * @docs https://clickhouse.com/docs/en/sql-reference/functions/type-conversion-functions/#type_conversion_function-cast
     */
    public static function cast(string $value, string $type): Expression
    {
        return new self(sprintf('CAST(%s AS %s)', $value, $type));
    }

}
