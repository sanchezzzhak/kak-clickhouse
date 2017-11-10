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
    /**
     * @event Event an event that is triggered after a DB connection is established
     */
    const EVENT_AFTER_OPEN = 'afterOpen';

    /**
     * @var string name use database default use value  "default"
     */
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
     * @var string
     */
    public $commandClass = 'kak\clickhouse\Command';
    public $schemaClass = 'kak\clickhouse\Schema';
    public $transportClass = 'yii\httpclient\CurlTransport';

    /**
     * @var array
     */
    public $requestConfig = [
        'class' => 'kak\clickhouse\httpclient\Request',
    ];

    public $schemaMap = [
        'clickhouse' => 'kak\clickhouse\Schema'
    ];


    /** @var bool|Client */
    private $_transport = false;

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

        /** @var $command \kak\clickhouse\Command  */
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
            'requestConfig' => $this->requestConfig,
        ]);
    }

    public function buildUrl($url, $data = [])
    {
        $parsed = parse_url($url);
        isset($parsed['query']) ? parse_str($parsed['query'], $parsed['query']) : $parsed['query'] = [];
        $params = isset($parsed['query']) ? array_merge($parsed['query'], $data) : $data;

        $parsed['query'] = !empty($params) ? '?' . http_build_query($params) : '';
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
     * Quotes a string value for use in a query.
     * Note that if the parameter is not a string or int, it will be returned without change.
     * @param string $str string to be quoted
     * @return string the properly quoted string
     */
    public function quoteValue($str)
    {
        return $this->getSchema()->quoteValue($str);
    }

    public function quoteSql($sql)
    {
        return $sql;
    }


    public function ping()
    {
        $this->open();
        $query = 'SELECT 1';
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
        if ($this->getIsActive()) {
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
