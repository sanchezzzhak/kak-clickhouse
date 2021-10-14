<?php

namespace kak\clickhouse;

use kak\clickhouse\httpclient\Request;
use Yii;
use yii\base\Exception;
use yii\db\Command as BaseCommand;
use yii\db\Exception as DbException;
use yii\helpers\ArrayHelper;
use yii\helpers\Json;
use yii\httpclient\Response;

/**
 * Class Command
 * @package kak\clickhouse
 * @property Connection $db
 */
class Command extends BaseCommand
{

    const FETCH = 'fetch';
    const FETCH_ALL = 'fetchAll';
    const FETCH_COLUMN = 'fetchColumn';
    const FETCH_SCALAR = 'fetchScalar';

    const FETCH_MODE_TOTAL = 7;
    const FETCH_MODE_ALL = 8;

    /** @var int fetch type result */
    public $fetchMode = 0;

    private $_format;

    private $_pendingParams = [];

    private $_is_result;

    private $_options = [];

    /**
     * @var
     */
    private $_meta;
    /**
     * @var
     */
    private $_data;
    /**
     * @var
     */
    private $_totals;
    /**
     * @var array
     */
    private $_extremes;
    /**
     * @var int
     */
    private $_rows;
    /**
     * @var array
     */
    private $_statistics;
    /**
     * @var string|null
     */
    private $_query_id;
    /**
     * @var string|null
     */
    private $_timezone;
    /**
     * @var string|null
     */
    private $_server_name;
    /**
     * @var array|null
     */
    private $_summary;

    /**
     * @var
     */
    private $_rows_before_limit_at_least;


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
     * @return array
     */
    public function getOptions()
    {
        return $this->_options;
    }

    /**
     * all replace settings
     * @param array $options
     * @return $this
     */
    public function setOptions($options)
    {
        $this->_options = $options;
        return $this;
    }

    /**
     * Adds more options to already defined ones.
     * Please refer to [[setOptions()]] on how to specify options.
     * @param array $options additional options
     * @return $this self reference.
     */
    public function addOptions(array $options)
    {
        foreach ($options as $key => $value) {
            if (is_array($value) && isset($this->_options[$key])) {
                $value = ArrayHelper::merge($this->_options[$key], $value);
            }
            $this->_options[$key] = $value;
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

    /**
     * @param bool $prepare
     * @return array|int|mixed
     * @throws Exception
     */
    public function execute($prepare = false)
    {
        $rawSql = $this->getRawSql();
        $response = $this->db->transport
            ->createRequest()
            ->setUrl($this->getBaseUrl())
            ->setMethod('POST')
            ->setContent($rawSql)
            ->send();

        $this->checkResponseStatus($response);

        if ($prepare) {
            return $this->parseResponse($response);
        }
        return $response;
    }


    /**
     * @return array|mixed
     * @throws Exception
     */
    public function queryColumn()
    {
        return $this->queryInternal(self::FETCH_COLUMN);
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
        $result = $this->queryInternal(self::FETCH_SCALAR, 0);
        return (is_numeric($result)) ? ($result + 0) : $result;
    }

    public function getRawSql()
    {
        if (empty($this->params)) {
            return $this->getSql();
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
            return strtr($this->getSql(), $params);
        }
        $sql = '';
        foreach (explode('?', $this->getSql()) as $i => $part) {
            $sql .= (isset($params[$i]) ? $params[$i] : '') . $part;
        }
        return $sql;
    }

    /**
     * @param string $method
     * @param null $fetchMode
     * @return array|DataReader|mixed
     * @throws Exception
     */
    protected function queryInternal($method, $fetchMode = null)
    {
        $rawSql = $this->getRawSql();

        // added limit 1 is fetch one
        if (($method === self::FETCH) && 0 === stripos($rawSql, "SELECT") && !preg_match('#LIMIT#is', $rawSql)) {
            $rawSql .= ' LIMIT 1';
        }
        // is format null set format json
        if ($this->getFormat() === null && strpos($rawSql, 'FORMAT ') === false) {
            $rawSql .= ' FORMAT JSON';
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
                    return $this->prepareResult($result[0], $method, $fetchMode);
                }
            }
        }
        
        $token = $rawSql;
        try {
            Yii::beginProfile($token, 'kak\clickhouse\Command::query');

            $response = $this->db->transport
                ->createRequest()
                ->setUrl($this->getBaseUrl())
                ->setMethod('POST')
                ->setContent($rawSql)
                ->send();

            $this->checkResponseStatus($response);

            $data = $this->parseResponse($response);
            $result = $this->prepareResult($data, $method, $fetchMode);

            Yii::endProfile($token, 'kak\clickhouse\Command::query');
        } catch (\Exception $e) {
            Yii::endProfile($token, 'kak\clickhouse\Command::query');
            throw new Exception("Query error: " . $e->getMessage());
        }

        if (isset($cache, $cacheKey, $info)) {
            $cache->set($cacheKey, [$data], $info[1], $info[2]);
            Yii::trace('Saved query result in cache', 'kak\clickhouse\Command::query');
        }

        return $result;
    }

    /**
     * @param $result
     * @return array
     * @throws DbException
     */
    protected function getStatementData($result)
    {
        return [
            'meta' => $this->getMeta(),
            'data' => $result,
            'rows' => $this->getRows(),
            'countAll' => $this->getCountAll(),
            'totals' => $this->getTotals(),
            'statistics' => $this->getStatistics(),
            'extremes' => $this->getExtremes(),
            'summary' => $this->getSummary(),
            'serverName' => $this->getServerName(),
            'timezone' => $this->getTimezone(),
            'queryId' => $this->getQueryId(),
        ];
    }


    protected function getBaseUrl()
    {
        $urlBase = $this->db->transport->baseUrl;
        return $this->db->buildUrl($urlBase, array_merge([
            'database' => $this->db->database
        ], $this->getOptions()));
    }

    /**
     * Raise exception when get 500s error
     * @param $response Response
     * @throws Exception
     */
    public function checkResponseStatus($response)
    {
        if ($response->getStatusCode() != 200) {
            throw new DbException($response->getContent());
        }
    }


    private function prepareResult($result, $method = null, $fetchMode = null)
    {
        $this->prepareResponseData($result);
        $result = ArrayHelper::getValue($result, 'data', []);
        switch ($method) {
            case self::FETCH_COLUMN:
                return array_map(function ($a) {
                    return array_values($a)[0];
                }, $result);
            case self::FETCH_SCALAR:
                if (array_key_exists(0, $result)) {
                    return current($result[0]);
                }
                break;
            case self::FETCH:
                return is_array($result) ? array_shift($result) : $result;
        }

        if ($fetchMode === self::FETCH_MODE_ALL) {
            return $this->getStatementData($result);
        }

        if ($fetchMode === self::FETCH_MODE_TOTAL) {
            return $this->getTotals();
        }

        return $result;
    }


    /**
     * Parse response with data
     * @param Response $response
     * @param null|string $method
     * @param bool $prepareResponse
     * @return mixed|array
     */
    private function parseResponse(Response $response)
    {
        $headers = $response->getHeaders();

        $contentType = $headers->get('Content-Type');
        list($type) = explode(';', $contentType);

        $type = strtolower($type);
        $hash = [
            'application/json' => 'parseJson'
        ];

        $prefix = 'x-clickhouse-';
        if (($queryId = $headers->get($prefix . 'query-id')) !== null) {
            $this->_query_id = $queryId;
        }
        if (($serverName = $headers->get($prefix .  'server-display-name')) !== null) {
            $this->_server_name = $serverName;
        }
        if (($timezone = $headers->get($prefix . 'timezone')) !== null) {
            $this->_timezone = $timezone;
        }
        if (($summary = $headers->get($prefix . 'summary')) !== null) {
            $this->_summary = Json::decode($summary);
        }
        return (isset($hash[$type])) ? $this->{$hash[$type]}($response->content) : $response->content;
    }

    /**
     * @param $result
     */
    private function prepareResponseData($result)
    {
        if (!is_array($result)) {
            return;
        }
        $this->_is_result = true;
        foreach (['meta', 'data', 'totals', 'extremes', 'rows', 'rows_before_limit_at_least', 'statistics'] as $key) {
            if (isset($result[$key])) {
                $attr = "_" . $key;
                $this->{$attr} = $result[$key];
            }
        }

        $columns = ArrayHelper::index($this->getMeta(), 'name');
        foreach ($this->getData() as $key => &$item) {
            $columnType = $columns[$key]['type'] ?? null;
            if ($columnType === null) {
                continue;
            }
            // cast string array to array
            if ($columns[$key] && stripos($columnType, 'Array') !== false) {
                $item = json_decode($item, true);
            }
        }
    }

    /**
     * parse json content to array
     * @param string $content
     * @return mixed|null
     */
    private function parseJson($content)
    {
        return Json::decode($content);
    }

    /**
     * checking that the request was executed otherwise an exception
     * @throws DbException
     */
    private function ensureQueryExecuted()
    {
        if (true !== $this->_is_result) {
            throw new DbException('Query was not executed yet');
        }
    }

    /**
     * get meta columns information
     * @return mixed
     * @throws DbException
     */
    public function getMeta()
    {
        $this->ensureQueryExecuted();
        return $this->_meta;
    }

    /**
     * get all data result
     * @return mixed|array
     * @throws DbException
     */
    public function getData()
    {
        if ($this->_is_result === null && !empty($this->getSql())) {
            $this->queryInternal(null);
        }
        $this->ensureQueryExecuted();
        return $this->_data;
    }

    /**
     * Generation sql `create table` for meta (only select query)
     *
     * ```php
     * $sql = 'SELECT sum(click) as sum_click, event_date FROM table_name GROUP BY event_date LIMIT 10';
     * $command = $connection->createCommand($sql);
     * $data = $command->queryAll();
     * $schemaSql = $command->getSchemaQuery();
     * ```
     *
     * @return string
     * @throws DbException
     */
    public function getSchemaQuery()
    {
        $sql = $this->getSql();
        $meta = $this->getMeta();
        if (!preg_match('#^SELECT#is', $sql)) {
            throw new DbException('Query was not SELECT type');
        }
        $table = "CREATE TABLE x (\n    ";
        $columns = [];
        foreach ($meta as $item) {
            $columns[] = '`' . $item['name'] . '` ' . $item['type'];
        }
        $table .= implode(",\n    ", $columns);
        $table .= "\n)";

        return $table;
    }

    /**
     * get clickhouse query id
     * @return mixed
     * @throws DbException
     */
    public function getQueryId()
    {
        $this->ensureQueryExecuted();
        return $this->_query_id;
    }

    /**g
     * get clickhouse server name
     * @return string|null
     * @throws DbException
     */
    public function getServerName()
    {
        $this->ensureQueryExecuted();
        return $this->_server_name;
    }

    /**
     * get clickhouse summary array
     * @return array|null
     * @throws DbException
     */
    public function getSummary()
    {
        $this->ensureQueryExecuted();
        return $this->_summary;
    }

    /**
     * get clickhouse timezone
     * @return string|null
     * @throws DbException
     */
    public function getTimezone()
    {
        $this->ensureQueryExecuted();
        return $this->_timezone;
    }

    /**
     * get total values​(when using WITH TOTALS).
     * @return mixed
     * @throws DbException
     */
    public function getTotals()
    {
        $this->ensureQueryExecuted();
        return $this->_totals;
    }

    /**
     * extreme values (with the extremes setting set to 1).
     * @return mixed
     * @throws DbException
     */
    public function getExtremes()
    {
        $this->ensureQueryExecuted();
        return $this->_extremes;
    }

    /**
     * get the total number of rows output.
     * @return mixed
     * @throws DbException
     */
    public function getRows()
    {
        $this->ensureQueryExecuted();
        return $this->_rows;
    }

    /**
     * at least how many lines would have turned out if there were no LIMIT. Displayed only if the query contains LIMIT
     * similarly mysql (SQL_CALC_FOUND_ROWS)
     * @return mixed
     * @throws DbException
     */
    public function getCountAll()
    {
        $this->ensureQueryExecuted();
        return $this->_rows_before_limit_at_least;
    }

    /**
     * result format array
     *   {
     *      "elapsed": 0.000070241,
     *       "rows_read": 3,
     *       "bytes_read": 24
     *   }
     *
     * @return mixed
     * @throws DbException
     */
    public function getStatistics()
    {
        $this->ensureQueryExecuted();
        return $this->_statistics;
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
     * @param null $columns columns default columns get schema table
     * @param array $files list files
     * @param string $format file format
     * @return Response[]
     */
    public function batchInsertFiles($table, $columns = null, $files = [], $format = 'CSV')
    {
        $categoryLog = 'kak\clickhouse\Command::batchInsertFiles';
        if ($columns === null) {
            $columns = $this->db->getSchema()->getTableSchema($table)->columnNames;
        }
        $sql = 'INSERT INTO ' . $this->db->getSchema()->quoteTableName($table) . ' (' . implode(', ', $columns) . ')' . ' FORMAT ' . $format;

        Yii::info($sql, $categoryLog);
        Yii::beginProfile($sql, $categoryLog);

        $urlBase = $this->db->transport->baseUrl;
        $requests = [];
        $url = $this->db->buildUrl($urlBase, [
            'database' => $this->db->database,
            'query' => $sql,
        ]);

        foreach ($files as $key => $file) {
            /** @var Request $request */
            $request = $this->makeBatchInsert($url, file_get_contents($file));
            $requests[$key] = $request;
        }

        $responses = $this->db->transport->batchSend($requests);
        /*foreach ($responses as $response){
           var_dump($response->getContent());
           var_dump($response->getHeaders());
           var_dump($response->getFormat());
        }*/
        Yii::beginProfile($sql);

        return $responses;
    }

    /**
     * @param $table
     * @param null $columns
     * @param array $files
     * @param string $format
     * @param int $size
     * @return Response[]
     */
    public function batchInsertFilesDataSize($table, $columns = null, $files = [], $format = 'CSV', $size = 10000)
    {
        $categoryLog = 'kak\clickhouse\Command::batchInsertFilesDataSize';
        if ($columns === null) {
            $columns = $this->db->getSchema()->getTableSchema($table)->columnNames;
        }
        $sql = 'INSERT INTO ' . $this->db->getSchema()->quoteTableName($table) . ' (' . implode(', ', $columns) . ')' . ' FORMAT ' . $format;

        Yii::info($sql, $categoryLog);
        Yii::beginProfile($sql, $categoryLog);

        $urlBase = $this->db->transport->baseUrl;
        $responses = [];
        $url = $this->db->buildUrl($urlBase, [
            'database' => $this->db->database,
            'query' => $sql,
        ]);
        foreach ($files as $key => $file) {
            if (($handle = fopen($file, 'r')) !== false) {
                $buffer = '';
                $count = $part = 0;
                while (($line = fgets($handle)) !== false) {
                    $buffer .= $line;
                    $count++;
                    if ($count >= $size) {
                        $responses[$key]['part_' . ($part++)] = ($this->makeBatchInsert($url, $buffer)->send());
                        $buffer = '';
                        $count = 0;
                    }
                }
                if (!empty($buffer)) {
                    $responses[$key]['part_' . ($part++)] = ($this->makeBatchInsert($url, $buffer)->send());
                }
                fclose($handle);
            }
        }
        Yii::beginProfile($sql);

        return $responses;
    }


    /**
     * @param $url
     * @param $data
     * @return Request
     */
    private function makeBatchInsert($url, $data)
    {
        /** @var Request $request */
        $request = $this->db->transport->createRequest();
        $request->setFullUrl($url);
        $request->setMethod('POST');
        $request->setContent($data);
        return $request;
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

    public function query()
    {
        throw new \yii\db\Exception('Clichouse unsupport cursor');
    }
}