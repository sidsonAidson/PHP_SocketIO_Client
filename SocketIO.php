<?php

/**
 * Created by PhpStorm.
 * User: mac
 * Date: 07/06/2017
 * Time: 13:09
 */

class SocketIO
{

    const  SSL_PROTOCOLE = 'ssl://';
    const  TLS_PROTOCOLE = 'tls://';
    const  NO_SECURE_PROTOCOLE = '';

    /**
     * @var string null
     */
    private $port;

    /**
     * @var string|int null
     */
    private $host;


    /**
     * @var string
     */
    private $protocole = SocketIO::NO_SECURE_PROTOCOLE;


    /**
     * @var string
     */
    private $event;

    /**
     * @var array| string
     */
    private $data = [];

    /**
     * @var string
     */
    private $transport;

    /**
     * @var string
     */
    private $path;

    private $errors = [];


    /**
     * @var int
     */
    private $maxRetry = 5;

    /**
     * @var int
     */
    private $retryInterval = 200;


    private $queryParams = [];


    /**
     * SocketIO constructor.
     * @param string null $host
     * @param string|int null $port
     * @param string $path
     */
    public function __construct($host = null, $port = null, $path = "/socket.io/?EIO=3")
    {
        $this->host = $host;
        $this->port = $port;
        $this->path = $path;
    }

    /**
     * @return array
     */
    public function getErrors()
    {
        return $this->errors;
    }



    /**
     * @return string
     */
    public function getPort()
    {
        return $this->port;
    }

    /**
     * @param string $port
     */
    public function setPort($port)
    {
        $this->port = intval($port);
    }

    /**
     * @return int|string
     */
    public function getHost()
    {
        return $this->host;
    }

    /**
     * @param int|string $host
     */
    public function setHost($host)
    {
        $this->host = $host;
    }



    /**
     * @return string
     */
    public function getEvent()
    {
        return $this->event;
    }

    /**
     * @param string $event
     */
    private function setEvent($event)
    {
        $this->event = $event;
    }

    /**
     * @return array|string
     */
    public function getData()
    {
        if(is_string($this->data))
        {
            return $this->data;
        }
        else{
            return json_encode($this->data);
        }
    }

    /**
     * @param array|string $data
     */
    public function setData($data)
    {
        $this->data = $data;

    }



    /**
     * @return string
     */
    public function getPath()
    {
        return $this->path;
    }

    /**
     * @param string $path
     */
    public function setPath($path)
    {
        $this->path = $path;
    }



    /**
     * @return int
     */
    public function getMaxRetry()
    {
        return $this->maxRetry;
    }

    /**
     * @param int $maxRetry
     */
    public function setMaxRetry($maxRetry)
    {
        $this->maxRetry = $maxRetry;
    }

    /**
     * @return int
     */
    public function getRetryInterval()
    {
        return $this->retryInterval;
    }

    /**
     * @param int $retryInterval
     */
    public function setRetryInterval($retryInterval)
    {
        $this->retryInterval = $retryInterval;
    }

    /**
     * @return string
     */
    public function getProtocole()
    {
        return $this->protocole;
    }

    /**
     * @param string $protocole
     */
    public function setProtocole( $protocole)
    {
        if(!in_array($protocole, [SocketIO::NO_SECURE_PROTOCOLE, SocketIO::SSL_PROTOCOLE, SocketIO::TLS_PROTOCOLE]))
        {
            $protocole = SocketIO::NO_SECURE_PROTOCOLE;
        }

        $this->protocole = $protocole;
    }



    /**
     * @return string
     */
    public function getQueryParams()
    {
        if(count($this->queryParams) > 0)
        {
            $query = http_build_query($this->queryParams);

        }

        return $query;
    }

    /**
     * @param array $queryParams
     */
    public function setQueryParams($queryParams)
    {
        $this->queryParams = $queryParams;
    }



    private function send()
    {
        set_error_handler(function($errno, $errstr, $errfile, $errline, $errcontext) {
            $error = [
                'message' => $errstr,
                'file' => $errfile,
                'line' => $errline
            ];

            if(!in_array($error, $this->errors))
            {
                array_push($this->errors, $error);
            }

        });

        $fd = fsockopen("{$this->protocole}{$this->host}", $this->port, $errno, $errstr);

        if (!$fd) {
            restore_error_handler();
            return false;
        }

        $key = $this->generateKey();
        $out = "GET {$this->path}&{$this->getQueryParams()}&transport=websocket HTTP/1.1\r\n";
        $out.= "Host: {$this->host}:{$this->port}\r\n";
        $out.= "Upgrade: WebSocket\r\n";
        $out.= "Connection: Upgrade\r\n";
        $out.= "Sec-WebSocket-Key: $key\r\n";
        $out.= "Sec-WebSocket-Version: 13\r\n";
        $out.= "Origin: *\r\n\r\n";

        fwrite($fd, $out);
        // 101 switching protocols, see if echoes key
        $result= fread($fd,10000);
    
        $matches = []; // For <PHP7: array()
        preg_match('#Sec-WebSocket-Accept:\s(.*)$#mU', $result, $matches);
        $keyAccept = trim($matches[1]);
        $expectedResonse = base64_encode(pack('H*', sha1($key . '258EAFA5-E914-47DA-95CA-C5AB0DC85B11')));
        $handshaked = ($keyAccept === $expectedResonse) ? true : false;

        if ($handshaked)
        {
            fwrite($fd, $this->hybi10Encode('42["' . $this->event . '", "' . addslashes($this->getData()) . '"]'));
            fread($fd,1000000);
            restore_error_handler();
            return true;
        }
        else
        {
            restore_error_handler();
            return false;
        }
    }


    public function emit($event, $data = [])
    {


        $this->setEvent($event);
        $this->setData($data);
        $success = false;

        begin:
        try {
            if ($this->send()) {
                $success = true;
            } else {
                do
                {
                    usleep($this->retryInterval * 1000);
                    $this->maxRetry--;
                    $success = $this->send();

                } while($this->maxRetry > 0 && !$success);

            }
        } catch (Exception $e) {
            if($this->maxRetry > 0)
            {
                goto begin;
            }
        }

        return $success;

    }

    private function generateKey($length = 16)
    {
        $c = 0;
        $tmp = '';
        while ($c++ * 16 < $length) { $tmp .= md5(mt_rand(), true); }
        return base64_encode(substr($tmp, 0, $length));
    }

    private function hybi10Encode($payload, $type = 'text', $masked = true)
    {
        $frameHead = array();
        $payloadLength = strlen($payload);
        switch ($type) {
            case 'text':
                $frameHead[0] = 129;
                break;
            case 'close':
                $frameHead[0] = 136;
                break;
            case 'ping':
                $frameHead[0] = 137;
                break;
            case 'pong':
                $frameHead[0] = 138;
                break;
        }
        if ($payloadLength > 65535) {
            $payloadLengthBin = str_split(sprintf('%064b', $payloadLength), 8);
            $frameHead[1] = ($masked === true) ? 255 : 127;
            for ($i = 0; $i < 8; $i++) {
                $frameHead[$i + 2] = bindec($payloadLengthBin[$i]);
            }
            if ($frameHead[2] > 127) {
//                $this->close(1004);
                return false;
            }
        } elseif ($payloadLength > 125) {
            $payloadLengthBin = str_split(sprintf('%016b', $payloadLength), 8);
            $frameHead[1] = ($masked === true) ? 254 : 126;
            $frameHead[2] = bindec($payloadLengthBin[0]);
            $frameHead[3] = bindec($payloadLengthBin[1]);
        } else {
            $frameHead[1] = ($masked === true) ? $payloadLength + 128 : $payloadLength;
        }

        foreach (array_keys($frameHead) as $i) {
            $frameHead[$i] = chr($frameHead[$i]);
        }

        $mask = null;
        if ($masked === true) {
            $mask = array();
            for ($i = 0; $i < 4; $i++) {
                $mask[$i] = chr(rand(0, 255));
            }
            $frameHead = array_merge($frameHead, $mask);
        }
        $frame = implode('', $frameHead);
        for ($i = 0; $i < $payloadLength; $i++) {
            $frame .= ($masked === true) ? $payload[$i] ^ $mask[$i % 4] : $payload[$i];
        }
        return $frame;
    }
}
