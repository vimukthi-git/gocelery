package gocelery

import (
	"encoding/base64"
	"encoding/json"
	"reflect"
	"sync"
	"time"
)

const (
	// defaultMaxTries is the default max retries.
	defaultMaxTries = 3

	// MaxRetries runs the task until task stop returning ErrTaskRetryable
	MaxRetries = 15

	// defaultBackOff with current tries for back off
	defaultBackOff = time.Second * 5
)

// TaskSettings can be passed to the task with specific overrides.
type TaskSettings struct {
	MaxTries uint      `json:"max_tries"`
	Delay    time.Time `json:"delay"`
}

// DefaultSettings returns the TaskSettings with all the default values and will be used if the Task.Settings is nil.
func DefaultSettings() *TaskSettings {
	return &TaskSettings{
		MaxTries: defaultMaxTries,
		Delay:    time.Now().UTC(),
	}
}

// Task represents a task gocelery receives from the client.
type Task struct {
	Name     string
	Args     []interface{}
	Kwargs   map[string]interface{}
	Settings *TaskSettings // if Settings is nil, we fallback to default values
}

// CeleryMessage is actual message to be sent to Redis
type CeleryMessage struct {
	Body            string                 `json:"body"`
	Headers         map[string]interface{} `json:"headers"`
	ContentType     string                 `json:"content_type"`
	Properties      CeleryProperties       `json:"properties"`
	ContentEncoding string                 `json:"content_encoding"`
}

func (cm *CeleryMessage) reset() {
	cm.Headers = nil
	cm.Body = ""
	cm.Properties.CorrelationID = generateUUID()
	cm.Properties.ReplyTo = generateUUID()
	cm.Properties.DeliveryTag = generateUUID()
}

var celeryMessagePool = sync.Pool{
	New: func() interface{} {
		return &CeleryMessage{
			Body:        "",
			Headers:     nil,
			ContentType: "application/json",
			Properties: CeleryProperties{
				BodyEncoding:  "base64",
				CorrelationID: generateUUID(),
				ReplyTo:       generateUUID(),
				DeliveryInfo: CeleryDeliveryInfo{
					Priority:   0,
					RoutingKey: "celery",
					Exchange:   "celery",
				},
				DeliveryMode: 2,
				DeliveryTag:  generateUUID(),
			},
			ContentEncoding: "utf-8",
		}
	},
}

func getCeleryMessage(encodedTaskMessage string) *CeleryMessage {
	msg := celeryMessagePool.Get().(*CeleryMessage)
	msg.Body = encodedTaskMessage
	return msg
}

func releaseCeleryMessage(v *CeleryMessage) {
	v.reset()
	celeryMessagePool.Put(v)
}

// CeleryProperties represents properties json
type CeleryProperties struct {
	BodyEncoding  string             `json:"body_encoding"`
	CorrelationID string             `json:"correlation_id"`
	ReplyTo       string             `json:"replay_to"`
	DeliveryInfo  CeleryDeliveryInfo `json:"delivery_info"`
	DeliveryMode  int                `json:"delivery_mode"`
	DeliveryTag   string             `json:"delivery_tag"`
}

// CeleryDeliveryInfo represents deliveryinfo json
type CeleryDeliveryInfo struct {
	Priority   int    `json:"priority"`
	RoutingKey string `json:"routing_key"`
	Exchange   string `json:"exchange"`
}

// GetTaskMessage retrieve and decode task messages from broker
func (cm *CeleryMessage) GetTaskMessage() *TaskMessage {
	// ensure content-type is 'application/json'
	if cm.ContentType != "application/json" {
		log.Info("unsupported content type " + cm.ContentType)
		return nil
	}
	// ensure body encoding is base64
	if cm.Properties.BodyEncoding != "base64" {
		log.Info("unsupported body encoding " + cm.Properties.BodyEncoding)
		return nil
	}
	// ensure content encoding is utf-8
	if cm.ContentEncoding != "utf-8" {
		log.Info("unsupported encoding " + cm.ContentEncoding)
		return nil
	}
	// decode body
	taskMessage, err := DecodeTaskMessage(cm.Body)
	if err != nil {
		log.Info("failed to decode task message")
		return nil
	}
	return taskMessage
}

// TaskMessage is celery-compatible message
type TaskMessage struct {
	ID       string                 `json:"id"`
	Task     string                 `json:"task"`
	Args     []interface{}          `json:"args"`
	Kwargs   map[string]interface{} `json:"kwargs"`
	Tries    uint                   `json:"tries"`
	Settings *TaskSettings          `json:"settings"`
}

func (tm *TaskMessage) reset() {
	tm.ID = generateUUID()
	tm.Task = ""
	tm.Args = nil
	tm.Kwargs = nil
	tm.Tries = 0
	tm.Settings = nil
}

var taskMessagePool = sync.Pool{
	New: func() interface{} {
		return &TaskMessage{
			ID:     generateUUID(),
			Tries:  0,
			Kwargs: nil,
		}
	},
}

func getTaskMessage(task Task) *TaskMessage {
	msg := taskMessagePool.Get().(*TaskMessage)
	msg.Task = task.Name
	msg.Args = task.Args
	msg.Kwargs = task.Kwargs

	if task.Settings == nil {
		task.Settings = DefaultSettings()
	}
	msg.Settings = task.Settings
	return msg
}

// isRetryable to check if the task is retryable again.
func (tm *TaskMessage) isRetryable() bool {
	return tm.Tries < tm.Settings.MaxTries
}

func releaseTaskMessage(v *TaskMessage) {
	v.reset()
	taskMessagePool.Put(v)
}

// DecodeTaskMessage decodes base64 encrypted body and return TaskMessage object
func DecodeTaskMessage(encodedBody string) (*TaskMessage, error) {
	body, err := base64.StdEncoding.DecodeString(encodedBody)
	if err != nil {
		return nil, err
	}
	message := taskMessagePool.Get().(*TaskMessage)
	err = json.Unmarshal(body, message)
	if err != nil {
		return nil, err
	}
	return message, nil
}

// Encode returns base64 json encoded string
func (tm *TaskMessage) Encode() (string, error) {
	jsonData, err := json.Marshal(tm)
	if err != nil {
		return "", err
	}
	encodedData := base64.StdEncoding.EncodeToString(jsonData)
	return encodedData, err
}

// ResultMessage is return message received from broker
type ResultMessage struct {
	Result interface{} `json:"result"`
	Error  string      `json:"error"`
}

func (rm *ResultMessage) reset() {
	rm.Result = nil
	rm.Error = ""
}

var resultMessagePool = sync.Pool{
	New: func() interface{} {
		return &ResultMessage{}
	},
}

func getResultMessage(val interface{}) *ResultMessage {
	msg := resultMessagePool.Get().(*ResultMessage)
	msg.Result = val
	return msg
}

func getReflectionResultMessage(val *reflect.Value) *ResultMessage {
	msg := resultMessagePool.Get().(*ResultMessage)
	msg.Result = GetRealValue(val)
	return msg
}

func releaseResultMessage(v *ResultMessage) {
	v.reset()
	resultMessagePool.Put(v)
}
