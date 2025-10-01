<?php

namespace kak\clickhouse\data;

use kak\clickhouse\Command;
use kak\clickhouse\Connection;
use kak\clickhouse\Query;
use yii\db\Expression;

/**
 * Class SqlDataProvider
 * @package kak\clickhouse\data
 */
class SqlDataProvider extends \yii\data\SqlDataProvider
{
    /**
     * get total count for meta data
     */
    public const COUNT_BEFORE_LIMIT = 1;
    /**
     * get total count for query
     */
    public const COUNT_QUERY = 2;


    /** @var string|Connection  */
    public $db = 'clickhouse';

    public $countMode = self::COUNT_BEFORE_LIMIT;

    /**
     * @var Command
     */
    private $command;

    /**
     * @return Command
     */
    public function getCommand(): Command
    {
        return $this->command;
    }

    public function setCommand(Command $command): void
    {
        $this->command = $command;
    }

    /**
     * @inheritdoc
     */
    protected function prepareModels()
    {
        $sort = $this->getSort();
        $pagination = $this->getPagination();
        if ($pagination === false && $sort === false) {
            $this->command = $this->db->createCommand($this->sql, $this->params);
            return $this->command->queryAll();
        }

        $sql = $this->sql;
        $orders = [];
        $limit = $offset = null;

        if ($sort !== false) {
            $orders = $sort->getOrders();
            $pattern = '/\s+order\s+by\s+([\w\s,\.]+)$/i';
            if (preg_match($pattern, $sql, $matches)) {
                array_unshift($orders, new Expression($matches[1]));
                $sql = preg_replace($pattern, '', $sql);
            }
        }

        if ($pagination !== false) {
            if (!$page = (int)\Yii::$app->request->get($pagination->pageParam,0)) {
                $page = 1;
            }
            $pagination->totalCount = $page * $pagination->getPageSize();
            $limit = $pagination->getLimit();
            $offset = $pagination->getOffset();
        }

        $sql = $this->db->getQueryBuilder()->buildOrderByAndLimit($sql, $orders, $limit, $offset);

        $this->command = $this->db->createCommand($sql, $this->params);
        return $this->command->queryAll();
    }


    /**
     * @return int|null|string
     */
    protected function prepareTotalCount()
    {
        $pagination = $this->getPagination();
        if ($pagination !== false) {
            if ($this->countMode === self::COUNT_BEFORE_LIMIT) {
                $pagination->totalCount = $this->command->getCountAll();
            }
            if ($this->countMode === self::COUNT_QUERY) {
                $pagination->totalCount = (new Query([
                    'from' => ['sub' => "({$this->sql})"],
                    'params' => $this->params,
                ]))->count('', $this->db);

            }
            return $pagination->totalCount;
        }

        return null;
    }

    public function setTotalCount($value)
    {
        parent::setTotalCount($value);
        $pagination = $this->getPagination();
        if ($pagination !== false) {
            $pagination->getPageSize();
        }
    }

}