//go:build js

package gnmi

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"syscall/js"
	"time"

	"github.com/openconfig/gnmi/proto/gnmi"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	netbird "github.com/netbirdio/netbird/client/embed"
)

const (
	dialTimeout      = 10 * time.Second
	subscribeTimeout = 30 * time.Second
)

// Client wraps gNMI functionality for WASM
type Client struct {
	nbClient *netbird.Client
}

// NewClient creates a new gNMI client
func NewClient(nbClient *netbird.Client) *Client {
	return &Client{nbClient: nbClient}
}

// Subscribe starts a gNMI subscription and calls the JS callback with updates.
// mode is "sample" (default) or "on_change"; sampleIntervalMs applies to sample mode.
func (c *Client) Subscribe(target string, paths []string, encoding string, callback js.Value, plaintext bool, skipVerify bool, mode string, sampleIntervalMs int) js.Value {
	return createPromise(func(resolve, reject js.Value) {
		ctx := context.Background()

		// Create gRPC connection through NetBird tunnel
		conn, err := c.dialGRPC(ctx, target, plaintext, skipVerify)
		if err != nil {
			reject.Invoke(js.ValueOf(fmt.Sprintf("dial failed: %v", err)))
			return
		}

		client := gnmi.NewGNMIClient(conn)

		// Build subscription request
		subscribeClient, err := client.Subscribe(ctx)
		if err != nil {
			conn.Close()
			reject.Invoke(js.ValueOf(fmt.Sprintf("subscribe failed: %v", err)))
			return
		}

		// Build subscribe request
		subReq := buildSubscribeRequest(paths, encoding, mode, sampleIntervalMs)
		if err := subscribeClient.Send(subReq); err != nil {
			conn.Close()
			reject.Invoke(js.ValueOf(fmt.Sprintf("send subscribe request failed: %v", err)))
			return
		}

		// Return a control object with stop() method
		stopChan := make(chan struct{})
		controlObj := js.Global().Get("Object").New()
		controlObj.Set("stop", js.FuncOf(func(_ js.Value, _ []js.Value) any {
			close(stopChan)
			conn.Close()
			return nil
		}))

		// Start receiving updates in background
		go func() {
			for {
				select {
				case <-stopChan:
					return
				default:
					resp, err := subscribeClient.Recv()
					if err != nil {
						callback.Invoke(js.ValueOf(map[string]interface{}{
							"error": err.Error(),
						}))
						return
					}

					// Convert response to JS object
					jsResp := convertSubscribeResponse(resp)
					callback.Invoke(jsResp)
				}
			}
		}()

		resolve.Invoke(controlObj)
	})
}

// Get performs a one-time gNMI Get request
func (c *Client) Get(target string, paths []string, plaintext bool, skipVerify bool) js.Value {
	return createPromise(func(resolve, reject js.Value) {
		ctx, cancel := context.WithTimeout(context.Background(), subscribeTimeout)
		defer cancel()

		conn, err := c.dialGRPC(ctx, target, plaintext, skipVerify)
		if err != nil {
			reject.Invoke(js.ValueOf(fmt.Sprintf("dial failed: %v", err)))
			return
		}
		defer conn.Close()

		client := gnmi.NewGNMIClient(conn)

		// Build Get request
		req := &gnmi.GetRequest{
			Path:     buildPaths(paths),
			Encoding: gnmi.Encoding_JSON_IETF,
		}

		resp, err := client.Get(ctx, req)
		if err != nil {
			reject.Invoke(js.ValueOf(fmt.Sprintf("get failed: %v", err)))
			return
		}

		// Convert to JS
		jsResp := convertGetResponse(resp)
		resolve.Invoke(jsResp)
	})
}

// dialGRPC creates a gRPC connection through the NetBird tunnel
// If plaintext is true, uses no TLS (for insecure gRPC ports like 50052)
// If plaintext is false and skipVerify is true, uses TLS but skips cert verification
func (c *Client) dialGRPC(ctx context.Context, target string, plaintext bool, skipVerify bool) (*grpc.ClientConn, error) {
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()

	// Custom dialer that routes through NetBird
	dialer := func(ctx context.Context, addr string) (net.Conn, error) {
		return c.nbClient.Dial(ctx, "tcp", addr)
	}

	opts := []grpc.DialOption{
		grpc.WithContextDialer(dialer),
	}

	if plaintext {
		// No TLS - for insecure gRPC servers
		opts = append(opts, grpc.WithInsecure())
	} else {
		// TLS with optional cert verification skip
		tlsConfig := &tls.Config{
			InsecureSkipVerify: skipVerify,
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	}

	return grpc.DialContext(dialCtx, target, opts...)
}

func buildSubscribeRequest(paths []string, encoding string, mode string, sampleIntervalMs int) *gnmi.SubscribeRequest {
	enc := gnmi.Encoding_JSON_IETF
	switch encoding {
	case "proto":
		enc = gnmi.Encoding_PROTO
	case "json":
		enc = gnmi.Encoding_JSON
	case "bytes":
		enc = gnmi.Encoding_BYTES
	}

	subMode := gnmi.SubscriptionMode_SAMPLE
	if mode == "on_change" {
		subMode = gnmi.SubscriptionMode_ON_CHANGE
	}

	interval := uint64(sampleIntervalMs) * uint64(time.Millisecond)
	if interval == 0 {
		interval = 10 * uint64(time.Second)
	}

	var subs []*gnmi.Subscription
	for _, p := range paths {
		sub := &gnmi.Subscription{
			Path: parsePath(p),
			Mode: subMode,
		}
		if subMode == gnmi.SubscriptionMode_SAMPLE {
			sub.SampleInterval = interval
		}
		subs = append(subs, sub)
	}

	return &gnmi.SubscribeRequest{
		Request: &gnmi.SubscribeRequest_Subscribe{
			Subscribe: &gnmi.SubscriptionList{
				Subscription: subs,
				Mode:         gnmi.SubscriptionList_STREAM,
				Encoding:     enc,
			},
		},
	}
}

func buildPaths(paths []string) []*gnmi.Path {
	var result []*gnmi.Path
	for _, p := range paths {
		result = append(result, parsePath(p))
	}
	return result
}

func parsePath(path string) *gnmi.Path {
	// Simple path parsing - splits on /
	// e.g., "/interfaces/interface[name=Ethernet1]/state/counters"
	// For now, just use origin-based path
	return &gnmi.Path{
		Origin: "openconfig",
		Elem:   parsePathElements(path),
	}
}

func parsePathElements(path string) []*gnmi.PathElem {
	// Simple implementation - split by / and create elements
	// Skip leading slash
	if len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}

	var elems []*gnmi.PathElem
	// Very basic parsing - doesn't handle keys properly yet
	for _, part := range splitPath(path) {
		if part != "" {
			elems = append(elems, &gnmi.PathElem{Name: part})
		}
	}
	return elems
}

func splitPath(path string) []string {
	var result []string
	var current string
	inBracket := false

	for _, c := range path {
		switch c {
		case '[':
			inBracket = true
			current += string(c)
		case ']':
			inBracket = false
			current += string(c)
		case '/':
			if !inBracket {
				if current != "" {
					result = append(result, current)
				}
				current = ""
			} else {
				current += string(c)
			}
		default:
			current += string(c)
		}
	}
	if current != "" {
		result = append(result, current)
	}
	return result
}

func convertSubscribeResponse(resp *gnmi.SubscribeResponse) js.Value {
	obj := js.Global().Get("Object").New()

	switch r := resp.Response.(type) {
	case *gnmi.SubscribeResponse_Update:
		obj.Set("type", "update")
		obj.Set("timestamp", r.Update.Timestamp)

		updates := js.Global().Get("Array").New()
		for _, u := range r.Update.Update {
			update := js.Global().Get("Object").New()
			update.Set("path", pathToString(u.Path))
			update.Set("value", typedValueToJS(u.Val))
			updates.Call("push", update)
		}
		obj.Set("updates", updates)

	case *gnmi.SubscribeResponse_SyncResponse:
		obj.Set("type", "sync")
		obj.Set("synced", r.SyncResponse)
	}

	return obj
}

func convertGetResponse(resp *gnmi.GetResponse) js.Value {
	results := js.Global().Get("Array").New()

	for _, notif := range resp.Notification {
		for _, u := range notif.Update {
			obj := js.Global().Get("Object").New()
			obj.Set("path", pathToString(u.Path))
			obj.Set("value", typedValueToJS(u.Val))
			obj.Set("timestamp", notif.Timestamp)
			results.Call("push", obj)
		}
	}

	return results
}

func pathToString(path *gnmi.Path) string {
	if path == nil {
		return ""
	}
	result := ""
	for _, elem := range path.Elem {
		result += "/" + elem.Name
		for k, v := range elem.Key {
			result += fmt.Sprintf("[%s=%s]", k, v)
		}
	}
	return result
}

func typedValueToJS(val *gnmi.TypedValue) js.Value {
	if val == nil {
		return js.Null()
	}

	switch v := val.Value.(type) {
	case *gnmi.TypedValue_StringVal:
		return js.ValueOf(v.StringVal)
	case *gnmi.TypedValue_IntVal:
		return js.ValueOf(v.IntVal)
	case *gnmi.TypedValue_UintVal:
		return js.ValueOf(v.UintVal)
	case *gnmi.TypedValue_BoolVal:
		return js.ValueOf(v.BoolVal)
	case *gnmi.TypedValue_FloatVal:
		return js.ValueOf(v.FloatVal)
	case *gnmi.TypedValue_JsonVal:
		// Parse JSON and return as JS object
		return js.Global().Get("JSON").Call("parse", string(v.JsonVal))
	case *gnmi.TypedValue_JsonIetfVal:
		return js.Global().Get("JSON").Call("parse", string(v.JsonIetfVal))
	default:
		return js.ValueOf(fmt.Sprintf("%v", val))
	}
}

func createPromise(handler func(resolve, reject js.Value)) js.Value {
	return js.Global().Get("Promise").New(js.FuncOf(func(_ js.Value, promiseArgs []js.Value) any {
		resolve := promiseArgs[0]
		reject := promiseArgs[1]
		go handler(resolve, reject)
		return nil
	}))
}

// CreateJSInterface creates the JavaScript interface for gNMI
func CreateJSInterface(client *Client) js.Value {
	obj := js.Global().Get("Object").New()

	// subscribe(target, paths, encoding, callback, plaintext, skipVerify, mode, sampleIntervalMs) -> {stop: () => void}
	obj.Set("subscribe", js.FuncOf(func(_ js.Value, args []js.Value) any {
		if len(args) < 4 {
			return js.ValueOf("error: requires target, paths, encoding, callback")
		}

		target := args[0].String()

		// Convert JS array to Go slice
		pathsArr := args[1]
		paths := make([]string, pathsArr.Length())
		for i := 0; i < pathsArr.Length(); i++ {
			paths[i] = pathsArr.Index(i).String()
		}

		encoding := args[2].String()
		callback := args[3]

		plaintext := false
		if len(args) > 4 && !args[4].IsUndefined() {
			plaintext = args[4].Bool()
		}

		skipVerify := false
		if len(args) > 5 && !args[5].IsUndefined() {
			skipVerify = args[5].Bool()
		}

		mode := "sample"
		if len(args) > 6 && args[6].Type() == js.TypeString {
			mode = args[6].String()
		}

		sampleIntervalMs := 0
		if len(args) > 7 && args[7].Type() == js.TypeNumber {
			sampleIntervalMs = args[7].Int()
		}

		return client.Subscribe(target, paths, encoding, callback, plaintext, skipVerify, mode, sampleIntervalMs)
	}))

	// get(target, paths, plaintext, skipVerify) -> Promise<results>
	obj.Set("get", js.FuncOf(func(_ js.Value, args []js.Value) any {
		if len(args) < 2 {
			return js.ValueOf("error: requires target, paths")
		}

		target := args[0].String()

		pathsArr := args[1]
		paths := make([]string, pathsArr.Length())
		for i := 0; i < pathsArr.Length(); i++ {
			paths[i] = pathsArr.Index(i).String()
		}

		plaintext := false
		if len(args) > 2 && !args[2].IsUndefined() {
			plaintext = args[2].Bool()
		}

		skipVerify := false
		if len(args) > 3 && !args[3].IsUndefined() {
			skipVerify = args[3].Bool()
		}

		return client.Get(target, paths, plaintext, skipVerify)
	}))

	return obj
}
