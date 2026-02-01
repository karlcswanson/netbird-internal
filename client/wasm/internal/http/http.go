//go:build js

package http

import (
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"strings"
	"syscall/js"
	"time"

	log "github.com/sirupsen/logrus"
	netbird "github.com/netbirdio/netbird/client/embed"
)

const (
	httpTimeout     = 30 * time.Second
	maxResponseSize = 1024 * 1024 // 1MB
)

// performRequest executes an HTTP request through NetBird and returns the response and body
func performRequest(nbClient *netbird.Client, method, url string, headers map[string]string, body []byte, insecureSkipVerify bool) (*http.Response, []byte, error) {
	httpClient := nbClient.NewHTTPClient()
	httpClient.Timeout = httpTimeout

	// Configure TLS if needed
	if insecureSkipVerify {
		if transport, ok := httpClient.Transport.(*http.Transport); ok {
			transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
		}
	}

	req, err := http.NewRequest(method, url, strings.NewReader(string(body)))
	if err != nil {
		return nil, nil, fmt.Errorf("create request: %w", err)
	}

	for key, value := range headers {
		req.Header.Set(key, value)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("request failed: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			log.Errorf("failed to close response body: %v", err)
		}
	}()

	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseSize))
	if err != nil {
		return nil, nil, fmt.Errorf("read response: %w", err)
	}

	return resp, respBody, nil
}

// ProxyRequest performs a proxied HTTP request through NetBird and returns a JavaScript object
func ProxyRequest(nbClient *netbird.Client, request js.Value) (js.Value, error) {
	url := request.Get("url").String()
	if url == "" {
		return js.Undefined(), fmt.Errorf("URL is required")
	}

	method := "GET"
	if methodVal := request.Get("method"); !methodVal.IsNull() && !methodVal.IsUndefined() {
		method = strings.ToUpper(methodVal.String())
	}

	var requestBody []byte
	if bodyVal := request.Get("body"); !bodyVal.IsNull() && !bodyVal.IsUndefined() {
		requestBody = []byte(bodyVal.String())
	}

	requestHeaders := make(map[string]string)
	if headersVal := request.Get("headers"); !headersVal.IsNull() && !headersVal.IsUndefined() && headersVal.Type() == js.TypeObject {
		headerKeys := js.Global().Get("Object").Call("keys", headersVal)
		for i := 0; i < headerKeys.Length(); i++ {
			key := headerKeys.Index(i).String()
			value := headersVal.Get(key).String()
			requestHeaders[key] = value
		}
	}

	insecureSkipVerify := false
	if insecureVal := request.Get("insecureSkipVerify"); !insecureVal.IsNull() && !insecureVal.IsUndefined() {
		insecureSkipVerify = insecureVal.Bool()
	}

	returnBytes := false
	if returnBytesVal := request.Get("returnBytes"); !returnBytesVal.IsNull() && !returnBytesVal.IsUndefined() {
		returnBytes = returnBytesVal.Bool()
	}

	resp, body, err := performRequest(nbClient, method, url, requestHeaders, requestBody, insecureSkipVerify)
	if err != nil {
		return js.Undefined(), err
	}

	result := js.Global().Get("Object").New()
	result.Set("status", resp.StatusCode)
	result.Set("statusText", resp.Status)

	if returnBytes {
		// Return body as Uint8Array for binary data
		jsBody := js.Global().Get("Uint8Array").New(len(body))
		js.CopyBytesToJS(jsBody, body)
		result.Set("body", jsBody)
	} else {
		// Return body as string (default, backwards compatible)
		result.Set("body", string(body))
	}

	headers := js.Global().Get("Object").New()
	for key, values := range resp.Header {
		if len(values) > 0 {
			headers.Set(strings.ToLower(key), values[0])
		}
	}
	result.Set("headers", headers)

	return result, nil
}
