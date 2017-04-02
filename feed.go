package coinbase

import (
	"fmt"
	"strconv"
	"time"

	log "github.com/Sirupsen/logrus"
	ws "github.com/gorilla/websocket"
)

// SubscribeFeed subscribes for GDAX ws feed.
//	ch - a channel, to where messages will be sent.
//	stopCh - a channel to cancel subscribtion. IT is possible to send to it, or close it.
//	products - list of products to subscribe.
// if there are auth creds in client's config, the clients connects to the authenticated feed.
// if there are 5 failed attempts to connect to the ws in a row, SubscribeFeed returns an error.
func (c *Client) SubscribeFeed(ch chan<- Message, stopCh <-chan struct{}, products ...string) error {
	const maxErrorsInSeq = 5
	var errNum int
	for {
		var wsDialer ws.Dialer
		wsConn, _, err := wsDialer.Dial(c.WsURL, nil)
		if err != nil {
			if errNum = errNum + 1; errNum >= maxErrorsInSeq {
				return err
			}
			log.Errorf("gdax: failed to dial ws: %v", err)
			time.Sleep(time.Second)
			continue
		}
		errNum = 0
		errCh := make(chan error, 1)
		go func() {
			errCh <- c.subscribeWs(wsConn, ch, products...)
		}()
		select {
		case err := <-errCh:
			log.Printf("gdax: ws subscribe error: %v", err)
		case <-stopCh:
			wsConn.Close()
			return nil
		}
	}
}

func (c *Client) subscribeWs(wsConn *ws.Conn, ch chan<- Message, products ...string) error {
	subscribe := map[string]interface{}{
		"type":        "subscribe",
		"product_ids": products,
	}
	if len(c.Key) > 0 && len(c.Secret) > 0 && len(c.Passphrase) > 0 {
		timestamp := strconv.FormatInt(time.Now().Unix(), 10)
		message := fmt.Sprintf("%s%s%s%s", timestamp, "GET", "/users/self", "")
		sig, err := c.generateSig(message, c.Secret)
		if err != nil {
			return err
		}
		subscribe["signature"] = sig
		subscribe["key"] = c.Key
		subscribe["passphrase"] = c.Passphrase
		subscribe["timestamp"] = timestamp
	}
	if err := wsConn.WriteJSON(subscribe); err != nil {
		return err
	}
	var message Message
	for {
		if err := wsConn.ReadJSON(&message); err != nil {
			return err
		}
		ch <- message
	}
}
