<?php

/**
 * Created by PhpStorm.
 * User: kak
 * Date: 03.05.2017
 * Time: 17:57
 */
namespace kak\clickhouse\data;

use yii\db\Expression;

class SqlDataProvider extends \yii\data\SqlDataProvider
{
    /**
     * @inheritdoc
     */
    protected function prepareModels()
    {
        $sort = $this->getSort();
        $pagination = $this->getPagination();
        if ($pagination === false && $sort === false) {
            return $this->db->createCommand($this->sql, $this->params)->queryAll();
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

        if (!$page = (int)\Yii::$app->request->get($pagination->pageParam,0)) {
            $page = 1;
        }

        if ($pagination !== false) {
            $pagination->totalCount = $page * $pagination->getPageSize();
            $limit = $pagination->getLimit();
            $offset = $pagination->getOffset();
        }

        $sql = $this->db->getQueryBuilder()->buildOrderByAndLimit($sql, $orders, $limit, $offset);

        $command = $this->db->createCommand($sql, $this->params);
        $result  = $command->queryAll();

        if ($pagination !== false) {
            $pagination->totalCount = $command->getCountAll();
            $pagination->getPageSize();
            $this->setTotalCount( $pagination->totalCount );
        }
        $this->getBehaviors();
        return $result;
    }


}