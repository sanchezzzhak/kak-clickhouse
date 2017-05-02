<?php
namespace kak\clickhouse;

use yii\base\Component;

use yii\caching\Cache;
use yii\httpclient\Client;
use Yii;

/**
 * Class Connection
 * @package app\helpers\clickhouse
 * @property \yii\httpclient\Client $transport
 */
class Connection extends \yii\db\Connection
{
    public $tablePrefix;
    /**
     * @event Event an event that is triggered after a DB connection is established
     */
    const EVENT_AFTER_OPEN = 'afterOpen';

    /**
     * @var string the username for establishing DB connection. Defaults to `null` meaning no username to use.
     */
    public $username;
    /**
     * @var string the password for establishing DB connection. Defaults to `null` meaning no password to use.
     */
    public $password;

    public $database;

    /**
     * @var string the hostname or ip address to use for connecting to the click-house server. Defaults to 'localhost'.
     */
    public $dsn = 'localhost';

    /**
     * @var integer the port to use for connecting to the click-house server. Default port is 8123.
     */
    public $port = 8123;

    /**
     * @var Cache|string the cache object or the ID of the cache application component
     * that is used for query caching.
     * @see enableQueryCache
     */
    public $queryCache = 'cache';

    /**
     * @var boolean whether to enable schema caching.
     * Note that in order to enable truly schema caching, a valid cache component as specified
     * by [[schemaCache]] must be enabled and [[enableSchemaCache]] must be set true.
     * @see schemaCacheDuration
     * @see schemaCacheExclude
     * @see schemaCache
     */
    public $enableSchemaCache = false;

    /**
     * @var boolean whether to enable query caching.
     * Note that in order to enable query caching, a valid cache component as specified
     * by [[queryCache]] must be enabled and [[enableQueryCache]] must be set true.
     * Also, only the results of the queries enclosed within [[cache()]] will be cached.
     * @see queryCache
     * @see cache()
     * @see noCache()
     */
    public $enableQueryCache = true;

    /**
     * @var integer the default number of seconds that query results can remain valid in cache.
     * Use 0 to indicate that the cached data will never expire.
     * Defaults to 3600, meaning 3600 seconds, or one hour. Use 0 to indicate that the cached data will never expire.
     * The value of this property will be used when [[cache()]] is called without a cache duration.
     * @see enableQueryCache
     * @see cache()
     */
    public $queryCacheDuration = 3600;

    /**
     * @var string
     */
    public $commandClass   = 'kak\clickhouse\Command';
    public $schemaClass    = 'kak\clickhouse\Schema';
    public $transportClass = 'yii\httpclient\CurlTransport';
    public $requestClass = 'kak\clickhouse\httpclient\Request';


    /** @var bool|Client */
    private $_transport = false;


    /**
     * @var array query cache parameters for the [[cache()]] calls
     */
    private $_queryCacheInfo = [];

    private $_schema;

    /**
     * @param $sql
     * @param array $params
     * @return \kak\clickhouse\Command
     */
    public function createCommand($sql = null, $params = [])
    {
        $this->open();
        \Yii::trace("Executing ClickHouse: {$sql}", __METHOD__);

        /** @var Command $command */
        $command = new $this->commandClass([
            'db' => $this,
            'sql' => $sql,
        ]);

        return $command->bindValues($params);
    }


    /**
     * @return bool|Client
     */
    public function getTransport()
    {
        return $this->_transport;
    }


    public function getIsActive()
    {
        return $this->_transport !== false;
    }

    public function open()
    {
        if ($this->getIsActive()) {
            return;
        }

        $auth = !empty($this->username) ? $this->username . ':' . $this->password  .'@' : '';
        $scheme = 'http';
        $url =  $scheme. '://' . $auth . $this->dsn. ':' . $this->port;

        $params = [];
        if (!empty($this->database)) {
            $params['database'] = $this->database;
        }
        if (count($params)) {
            $url.= '?' . http_build_query($params);
        }
        $this->_transport = new Client([
            'baseUrl'   => $url,
            'transport' => $this->transportClass,
            'requestConfig' => [
                'class' => $this->requestClass,
            ]
        ]);
    }

    public function buildUrl($url, $data = [])
    {
        $parsed = parse_url($url);
        isset($parsed['query']) ? parse_str($parsed['query'], $parsed['query']) : $parsed['query'] = [];
        $params = isset($parsed['query']) ? array_merge($parsed['query'], $data) : $data;
        $parsed['query'] = ($params) ? '?' . http_build_query($params) : '';
        if (!isset($parsed['path'])) {
            $parsed['path'] = '/';
        }

        $auth =  (!empty($parsed['user']) ? $parsed['user'] : '') . (!empty($parsed['pass']) ? ':' . $parsed['pass'] : '');
        $defaultScheme = 'http';
        return (isset($parsed['scheme']) ? $parsed['scheme'] : $defaultScheme)
        . '://'
        . (!empty($auth) ? $auth . '@' : '')
        . $parsed['host']
        . (!empty($parsed['port']) ? ':' . $parsed['port'] : '')
        . $parsed['path']
        . $parsed['query'];
    }


    /**
     * Returns the current query cache information.
     * This method is used internally by [[Command]].
     * @param integer $duration the preferred caching duration. If null, it will be ignored.
     * @param \yii\caching\Dependency $dependency the preferred caching dependency. If null, it will be ignored.
     * @return array the current query cache information, or null if query cache is not enabled.
     */
    public function getQueryCacheInfo($duration, $dependency)
    {
        if (!$this->enableQueryCache) {
            return null;
        }

        $info = end($this->_queryCacheInfo);
        if (is_array($info)) {
            if ($duration === null) {
                $duration = $info[0];
            }
            if ($dependency === null) {
                $dependency = $info[1];
            }
        }

        if ($duration === 0 || $duration > 0) {
            if (is_string($this->queryCache) && Yii::$app) {
                $cache = \Yii::$app->get($this->queryCache, false);
            } else {
                $cache = $this->queryCache;
            }
            if ($cache instanceof Cache) {
                return [$cache, $duration, $dependency];
            }
        }

        return null;
    }



    /**
     * Quotes a string value for use in a query.
     * Note that if the parameter is not a string or int, it will be returned without change.
     * @param string $str string to be quoted
     * @return string the properly quoted string
     */
    public function quoteValue($str)
    {
        if (!is_string($str) && !is_int($str)) {
            return $str;
        }
        return "'" . addcslashes($str, "\000\n\r\\\032\047") . "'";
    }


    public function quoteSql($sql)
    {
        return $sql;
    }


    public function ping()
    {
        $this->open();
        $query = 'SELECT 1 FROM';
        $response = $this->transport
            ->createRequest()
            ->setHeaders(['Content-Type: application/x-www-form-urlencoded'])
            ->setMethod('POST')
            ->setContent($query)
            ->send();
        return trim($response->content) == '1';
    }


    /**
     * Closes the connection when this component is being serialized.
     * @return array
     */
    public function __sleep()
    {
        $this->close();
        return array_keys(get_object_vars($this));
    }


    /**
     * Closes the currently active DB connection.
     * It does nothing if the connection is already closed.
     */
    public function close()
    {
        if ($this->_transport !== false) {
            $connection = ($this->dsn . ':' . $this->port);
            \Yii::trace('Closing DB connection: ' . $connection, __METHOD__);
        }
    }

    /**
     * Initializes the DB connection.
     * This method is invoked right after the DB connection is established.
     * The default implementation triggers an [[EVENT_AFTER_OPEN]] event.
     */
    protected function initConnection()
    {
        $this->trigger(self::EVENT_AFTER_OPEN);
    }

    /**
     * @return object|\kak\clickhouse\Schema
     * @throws \yii\base\InvalidConfigException
     */
    public function getSchema()
    {
        return $this->_schema = Yii::createObject([
            'class' => $this->schemaClass,
            'db' => $this
        ]);
    }

    public function quoteTableName($name)
    {
        return $name;
    }

    public function getDriverName()
    {
        return 'clickhouse';
    }


    public function quoteColumnName($name)
    {
        return $name;
    }
    /**
     * Returns the query builder for the current DB connection.
     * @return QueryBuilder the query builder for the current DB connection.
     */
    public function getQueryBuilder()
    {
        return $this->getSchema()->getQueryBuilder();
    }
}
