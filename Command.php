<?php

namespace kak\clickhouse;
use yii\base\Component;
use Yii;
use yii\base\Exception;
use yii\helpers\ArrayHelper;
use yii\helpers\Json;

class Command extends Component
{
    /*** @var Connection */
    public $db;

    public $params = [];

    /**
     * @var integer the default number of seconds that query results can remain valid in cache.
     * Use 0 to indicate that the cached data will never expire. And use a negative number to indicate
     * query cache should not be used.
     * @see cache()
     */
    public $queryCacheDuration;
    /**
     * @var \yii\caching\Dependency the dependency to be associated with the cached query result for this command
     * @see cache()
     */
    public $queryCacheDependency;


    private $_sql;

    private $_pendingParams = [];


    /**
     * Enables query cache for this command.
     * @param integer $duration the number of seconds that query result of this command can remain valid in the cache.
     * If this is not set, the value of [[Connection::queryCacheDuration]] will be used instead.
     * Use 0 to indicate that the cached data will never expire.
     * @param \yii\caching\Dependency $dependency the cache dependency associated with the cached query result.
     * @return $this the command object itself
     */
    public function cache($duration = null, $dependency = null)
    {
        $this->queryCacheDuration = $duration === null ? $this->db->queryCacheDuration : $duration;
        $this->queryCacheDependency = $dependency;
        return $this;
    }

    /**
     * Disables query cache for this command.
     * @return $this the command object itself
     */
    public function noCache()
    {
        $this->queryCacheDuration = -1;
        return $this;
    }

    /**
     * Returns the SQL statement for this command.
     * @return string the SQL statement to be executed
     */
    public function getSql()
    {
        return $this->_sql;
    }

    /**
     * Specifies the SQL statement to be executed.
     * The previous SQL execution (if any) will be cancelled, and [[params]] will be cleared as well.
     * @param string $sql the SQL statement to be set.
     * @return $this this command instance
     */
    public function setSql($sql)
    {
        if ($sql !== $this->_sql) {
            $this->_sql = $this->db->quoteSql($sql);
            $this->_pendingParams = [];
            $this->params = [];
        }
        return $this;
    }



    public function bindValues($values)
    {
        if (empty($values)) {
            return $this;
        }

        //$schema = $this->db->getSchema();
        foreach ($values as $name => $value) {
            if (is_array($value)) {
                $this->_pendingParams[$name] = $value;
                $this->params[$name] = $value[0];
            } else {
                $this->params[$name] = $value;
            }
        }

        return $this;
    }


    public function execute($raw = false)
    {
       $rawSql = $this->getRawSql();
       $response =  $this->db->transport
           ->createRequest()
           ->setMethod('POST')
           ->setContent($rawSql)
           ->send();
       return !$raw ? $response : $this->parseResponse($response);
    }

    public function queryAll($fetchMode = null)
    {
        return $this->queryInternal('fetchAll', $fetchMode);
    }


    public function queryOne($fetchMode = null)
    {
        return $this->queryInternal('fetch', $fetchMode);
    }

    public function getRawSql()
    {
        if (empty($this->params)) {
            return $this->_sql;
        }
        $params = [];
        foreach ($this->params as $name => $value) {
            if (is_string($name) && strncmp(':', $name, 1)) {
                $name = ':' . $name;
            }
            if (is_string($value)) {
                $params[$name] = $this->db->quoteValue($value);
            } elseif (is_bool($value)) {
                $params[$name] = ($value ? 'TRUE' : 'FALSE');
            } elseif ($value === null) {
                $params[$name] = 'NULL';
            } elseif (!is_object($value) && !is_resource($value)) {
                $params[$name] = $value;
            }
        }
        if (!isset($params[1])) {
            return strtr($this->_sql, $params);
        }
        $sql = '';
        foreach (explode('?', $this->_sql) as $i => $part) {
            $sql .= (isset($params[$i]) ? $params[$i] : '') . $part;
        }
        return $sql;
    }


    protected function queryInternal($method, $fetchMode = null)
    {
        $rawSql = $this->getRawSql();
        if($method == 'fetch') {
            if(preg_match('#^SELECT#is',$rawSql) && !preg_match('#LIMIT#is',$rawSql)){
                $rawSql.=' LIMIT 1';
            }
        }

        \Yii::info($rawSql, 'kak\clickhouse\Command::query');

        if ($method !== '') {
            $info = $this->db->getQueryCacheInfo($this->queryCacheDuration, $this->queryCacheDependency);
            if (is_array($info)) {
                /* @var $cache \yii\caching\Cache */
                $cache = $info[0];
                $cacheKey = [
                    __CLASS__,
                    $method,
                    $fetchMode,
                    $this->db->dsn,
                    $this->db->username,
                    $rawSql,
                ];
                $result = $cache->get($cacheKey);
                if (is_array($result) && isset($result[0])) {
                    Yii::trace('Query result served from cache', 'kak\clickhouse\Command::query');
                    return $result[0];
                }
            }
        }


        $token = $rawSql;
        try {
            Yii::beginProfile($token, 'kak\clickhouse\Command::query');

            $response =  $this->db->transport
                ->createRequest()
                ->setMethod('POST')
                ->setContent($rawSql)
                ->send();

            if($response->getStatusCode() == 500 ) {
                throw new Exception($response->content);
            }
            $result = $this->parseResponse($response);

            Yii::endProfile($token, 'kak\clickhouse\Command::query');
        } catch (\Exception $e) {
            Yii::endProfile($token, 'kak\clickhouse\Command::query');
            throw new Exception("Query error: ".$e->getMessage());
        }
        return $result;
    }

    /**
     * Parse response with data
     * @param \yii\httpclient\Response $response
     * @return mixed|array
     */
    public function parseResponse(\yii\httpclient\Response $response)
    {
        $contentType = $response
            ->getHeaders()
            ->get('Content-Type');

        list($type) = explode(';',$contentType);

        $type = strtolower($type);
        $hash = [
            'application/json' => 'parseJson'
        ];

        return (isset($hash[$type]))? $this->{$hash[$type]}($response->content) : $response->content;
    }

    protected function parseJson($content)
    {
        $json = Json::decode($content);
        return ArrayHelper::getValue($json,'data');
    }


    /**
     * Creates an INSERT command.
     * For example,
     *
     * ```php
     * $connection->createCommand()->insert('user', [
     *     'name' => 'Sam',
     *     'age' => 30,
     * ])->execute();
     * ```
     *
     * The method will properly escape the column names, and bind the values to be inserted.
     *
     * Note that the created command is not executed until [[execute()]] is called.
     *
     * @param string $table the table that new rows will be inserted into.
     * @param array $columns the column data (name => value) to be inserted into the table.
     * @return $this the command object itself
     */
    public function insert($table, $columns)
    {
        $params = [];
        $sql = $this->db->getQueryBuilder()->insert($table, $columns, $params);
        return $this->setSql($sql)->bindValues($params);
    }

    public function batchInsert($table, $columns, $rows)
    {



       // return $this->db->transport->batchSend($requests);

       // return $this->setSql($sql);
    }



}