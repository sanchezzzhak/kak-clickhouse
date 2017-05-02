<?php

namespace kak\clickhouse;

use yii\base\Component;
use Yii;
use yii\base\Exception;
use yii\db\Command as BaseCommand;
use yii\db\Exception as DbException;
use yii\helpers\ArrayHelper;
use yii\helpers\Json;

class Command extends BaseCommand
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
    
    private $_format = null;
    private $_pendingParams = [];
    
    
    /**
     * @return null
     */
    public function getFormat()
    {
        return $this->_format;
    }
    
    /**
     * @param null $format
     * @return $this
     */
    public function setFormat($format)
    {
        $this->_format = $format;
        return $this;
    }
    
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
        
        $this->checkResponseStatus($response);
        
        if ($raw) {
            return $this->parseResponse($response);
        }
        return $response;
    }
    
    public function queryAll($fetchMode = null)
    {
        return $this->queryInternal('fetchAll', $fetchMode);
    }
    
    public function queryOne($fetchMode = null)
    {
        return $this->queryInternal('fetch', $fetchMode);
    }
    
    public function queryColumn()
    {
        return $this->queryInternal('fetchColumn');
    }
    
    /**
     * Executes the SQL statement and returns the value of the first column in the first row of data.
     * This method is best used when only a single value is needed for a query.
     * @return string|null|false the value of the first column in the first row of the query result.
     * False is returned if there is no value.
     * @throws Exception execution failed
     */
    public function queryScalar()
    {
        $result = $this->queryInternal('fetchScalar', 0);
        return (is_numeric($result)) ? ( $result + 0 ) : $result;
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
        if ($method == 'fetch') {
            if (preg_match('#^SELECT#is', $rawSql) && !preg_match('#LIMIT#is', $rawSql)) {
                $rawSql.=' LIMIT 1';
            }
        }
        if ($this->getFormat()===null && strpos($rawSql, 'FORMAT ')===false) {
            $rawSql.=' FORMAT JSON';
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
            
            $this->checkResponseStatus($response);
            $result = $this->parseResponse($response, $method);
            
            Yii::endProfile($token, 'kak\clickhouse\Command::query');
        } catch (\Exception $e) {
            Yii::endProfile($token, 'kak\clickhouse\Command::query');
            throw new Exception("Query error: ".$e->getMessage());
        }
        return $result;
    }
    
    /**
     * Raise exception when get 500s error
     * @param $response \yii\httpclient\Response
     * @throws Exception
     */
    public function checkResponseStatus($response)
    {
        if ($response->getStatusCode() != 200) {
            throw new DbException($response->getContent());
        }
    }
    
    
    /**
     * Parse response with data
     * @param \yii\httpclient\Response $response
     * @return mixed|array
     */
    public function parseResponse(\yii\httpclient\Response $response, $method = null)
    {
        $contentType = $response
            ->getHeaders()
            ->get('Content-Type');
        
        list($type) = explode(';', $contentType);
        
        $type = strtolower($type);
        $hash = [
            'application/json' => 'parseJson'
        ];
        $result = (isset($hash[$type]))? $this->{$hash[$type]}($response->content) : $response->content;
        
        switch ($method) {
            case 'fetchColumn':
                return array_map(function ($a) {
                    return array_values($a)[0];
                }, $result);
                break;
            case 'fetchScalar':
                if (array_key_exists(0, $result)) {
                    return current($result[0]);
                }
                break;
            case 'fetch':
                return is_array($result) ? array_shift($result) : $result;
                break;
        }        
        return  $result;
    }
    
    protected function parseJson($content)
    {
        $json = Json::decode($content);
        return ArrayHelper::getValue($json, 'data', []);
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

    /**
     * @param $table
     * @param null $columns
     * @param array $files
     * @param string $format
     * @return \yii\httpclient\Response[]
     */
    public function batchInsertFiles($table, $columns = null, $files = [], $format = 'CSV')
    {
        $categoryLog = 'kak\clickhouse\Command::batchInsertFiles';
        $schemaColumns = $this->db->getSchema()->getTableSchema($table)->columns;
        if ($columns === null) {
            $columns = $this->db->getSchema()->getTableSchema($table)->columnNames;
        }

        $structure = [];
        foreach ($columns as $column) {
            if(!isset($schemaColumns[$column])){
                continue;
            }
            $structure[] = $column . ' ' . $schemaColumns[$column]->dbType;
        }

        $sql =  'INSERT INTO '
            . $this->db->getSchema()->quoteTableName($table)
            .  ' (' . implode(', ', $columns) . ')'
            . ' FORMAT ' . $format;

        Yii::info($sql, $categoryLog);
        Yii::beginProfile($sql, $categoryLog);

        $urlBase = $this->db->transport->baseUrl;
        $requests = [];
        foreach ($files as $file) {
            $request = $this->db->transport->createRequest();

            $url = $this->db->buildUrl($urlBase,[
                'database' => $this->db->database,
                'query' => $sql,
                $table.'_structure' => implode(',', $structure),
                $table.'_format' => $format,
            ]);

            $request->setFullUrl($url);
            $request->setMethod('POST');
            $request->addFile($table, $file);
            $requests[] = $request;
        }
        $responses = $this->db->transport->batchSend($requests);
        //foreach ($responses as $response){
        //   var_dump($response->getContent());
        //   var_dump($response->getHeaders());
        //   var_dump($response->getFormat());
        //}

        Yii::beginProfile($sql);


        return $responses;
    }

    /**
     * Creates a batch INSERT command.
     * For example,
     *
     * ```php
     * $connection->createCommand()->batchInsert('user', ['name', 'age'], [
     *     ['Tom', 30],
     *     ['Jane', 20],
     *     ['Linda', 25],
     * ])->execute();
     * ```
     */
    public function batchInsert($table, $columns, $rows)
    {
        $sql = $this->db->getQueryBuilder()->batchInsert($table, $columns, $rows);
        return $this->setSql($sql);
    }
}