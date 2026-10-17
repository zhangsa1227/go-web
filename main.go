package main

import (
	"encoding/json"
	"log"
	"net/http"

	"github.com/gorilla/websocket"
)

// 1. http连接
// 2. 升级为websocket连接
// 3. 读取消息
// 4. 写回去
// 发送消息
type SendMsg struct {
	Type    string `json:"type"`
	Message string `json:"message"`
}

// 回复消息
type ReplyMsg struct {
	Type    string `json:"type"`
	Message string `json:"message"`
	From    string `json:"from"`
}

type Conn struct {
	wc   *websocket.Conn
	send chan ReplyMsg
}

func NewConn() {
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		// 2. 升级为websocket连接
		upgrader := NewUpgrader()
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			http.Error(w, "Failed to upgrade to websocket", http.StatusInternalServerError)
			return
		}

		// 4. 回复消息
		newConn := &Conn{wc: conn, send: make(chan ReplyMsg)}

		go newConn.Write()
		newConn.Read()
	})

	// 5. 监听服务
	if err := http.ListenAndServe(":9001", nil); err != nil {
		log.Fatal("ListenAndServe failed:", err)
	}
}

func NewUpgrader() *websocket.Upgrader {
	return &websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
	}
}

// 发送消息
func SendMessage(conn *websocket.Conn, msg SendMsg) error {
	msgByte, err := json.Marshal(msg)
	if err != nil {
		log.Fatal("sendMsg marshal fail", err)
		return err
	}
	return conn.WriteMessage(websocket.TextMessage, msgByte)
}

// 回复消息
func ReplyMessage(conn *websocket.Conn, msg ReplyMsg) error {
	msgByte, err := json.Marshal(msg)
	if err != nil {
		log.Fatal("sendMsg marshal fail", err)
		return err
	}
	return conn.WriteMessage(websocket.TextMessage, msgByte)
}

// 写数据到通道
func (c *Conn) Write() {
	defer c.wc.Close()

	for msg := range c.send {
		if err := ReplyMessage(c.wc, msg); err != nil {
			log.Fatal("发送回复失败:", err)
			return
		}
	}
}

// 从通道读取数据
func (c *Conn) Read() {
	defer close(c.send)

	for {
		var sendMsg SendMsg
		// 读取发送到wc.conn中消息
		err := c.wc.ReadJSON(&sendMsg)
		if err != nil {
			log.Fatal("读取消息结束:", err)
			return
		}

		// 将消息解析为ReplyMsg结构体
		replyMsg := ReplyMsg{
			Type:    "reply",
			Message: "服务端收到：" + sendMsg.Message,
			From:    "server",
		}

		c.send <- replyMsg
	}
}

func main() {
	NewConn()
}
