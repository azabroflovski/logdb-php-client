<?php

namespace LogDB;

use GuzzleHttp\Client;
use GuzzleHttp\Exception\RequestException;

class LogDB
{
    private $handlers = [];
    private $options;
    private $logDB;
    public $lastEventId = 0;

    public function __construct($options)
    {
        $this->options = (object) $options;
        $this->createApiClient();
    }

    private function createApiClient()
    {
        $this->logDB = new Client([
            'base_uri' => $this->options->url,
            'headers' => [
                'Authorization' => 'Bearer ' . $this->options->token,
                'Content-Type' => 'application/json',
            ]
        ]);
    }

    private function registerHandler($name, $callback)
    {
        $this->handlers[$name] = $callback;
    }

    public function on($type, $handler)
    {
        $this->registerHandler($type, $handler);
    }

    public function emit($type, $payload)
    {
        return $this->writeEvents([
            [
                'evt_type' => $type,
                'timestamp' => $this->timestamp(),
                'payload' => json_encode($payload)
            ]
        ]);
    }

    public function emitBulk($type, $payload)
    {
        $currentTimestamp = $this->timestamp();
        $events = array_map(function ($data) use ($type, $currentTimestamp) {
            return [
                'evt_type' => $type,
                'timestamp' => $currentTimestamp,
                'payload' => json_encode($data)
            ];
        }, $payload);

        return $this->writeEvents($events);
    }

    public function rawEmit($event)
    {
        return $this->writeEvents([$event]);
    }

    public function rawEmitBulk($events)
    {
        return $this->writeEvents($events);
    }

    public function fetchEvents($limit = 1000)
    {
        try {
            $response = $this->logDB->get("/events/{$limit}");
            return json_decode($response->getBody(), true);
        } catch (RequestException $e) {
            if ($e->hasResponse() && $e->getResponse()->getStatusCode() === 409) {
                $this->nack();
                return $this->fetchEvents($limit);
            }
            throw $e;
        }
    }

    public function writeEvents($events)
    {
        echo 'writing events', json_encode($events);
        $response = $this->logDB->post('/events', [
            'json' => $events
        ]);
        return $response->getStatusCode() === 200;
    }

    public function ack($eventId)
    {
        $response = $this->logDB->post("/ack/{$eventId}");
        return $response->getStatusCode() === 200;
    }

    public function nack()
    {
        $response = $this->logDB->post('/nack');
        return $response->getStatusCode() === 200;
    }

    private function eventReceiver($events)
    {
        echo 'Received ', count($events), ' events';
        if (isset($this->handlers['response'])) {
            call_user_func($this->handlers['response'], $events);
        }

        foreach ($events as $event) {
            if ($event['id'] > $this->lastEventId) {
                $this->lastEventId = $event['id'];

                if (isset($this->handlers['event'])) {
                    $eventContext = [
                        'id' => $event['id'],
                        'type' => $event['evt_type'],
                        'payload' => $event['payload'],
                        'createdAt' => $event['timestamp'],
                        'processed' => false,
                    ];

                    try {
                        call_user_func($this->handlers['event'], $eventContext);
                    } catch (Exception $e) {
                        if (isset($this->handlers['fail'])) {
                            call_user_func($this->handlers['fail'], $e, $eventContext);
                        }
                    }
                }
            }
        }
    }

    public function listen()
    {
        echo 'Waiting for events from ', $this->options->url;
        while (true) {
            try {
                $events = $this->fetchEvents();
                if (!empty($events)) {
                    $this->eventReceiver($events);
                }
            } catch (RequestException $e) {
                if ($e->hasResponse() && $e->getResponse()->getStatusCode() === 409) {
                    $this->nack();
                }
            }

            $this->ack($this->lastEventId);
            sleep(5);
        }
    }

    private function timestamp()
    {
        return time();
    }
}
