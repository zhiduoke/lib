package liteconsul

import (
	"bytes"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	methodGET = iota
	methodPUT
	methodDELETE
)

var methods = [...]string{
	methodGET:    http.MethodGet,
	methodPUT:    http.MethodPut,
	methodDELETE: http.MethodDelete,
}

func readBody(resp *http.Response) []byte {
	data, _ := ioutil.ReadAll(resp.Body)
	_ = resp.Body.Close()
	return data
}

func discardBody(resp *http.Response) {
	// ioutil.Discard 实现了 ReadFrom，不需要指定 buffer
	_, _ = io.Copy(ioutil.Discard, resp.Body)
	_ = resp.Body.Close()
}

type Error struct {
	Code    int
	Content string
}

func (e *Error) Error() string {
	return e.Content
}

func IsNotFound(err error) bool {
	if e, ok := err.(*Error); ok && e.Code == 404 {
		return true
	}
	return false
}

func errorFrom(resp *http.Response) error {
	return &Error{Code: resp.StatusCode, Content: string(readBody(resp))}
}

type QueryOptions struct {
	LastIndex uint64
	WaitTime  time.Duration
}

type QueryMetadata struct {
	LastIndex uint64
}

type consulRequest struct {
	method int // GET/PUT/DELETE
	path   string
	params []string
	body   []byte
}

type Client struct {
	addr  string
	token string
}

func (c *Client) send(r *consulRequest) (*http.Response, error) {
	var body io.Reader
	if len(r.body) > 0 {
		body = bytes.NewReader(r.body)
	}
	u := strings.Builder{}
	n := len(c.addr) + len(r.path)
	for i := 0; i+1 < len(r.params); i += 2 {
		n = 1 + len(r.params[i]) + 1 + len(r.params[i+1])
	}
	u.Grow(n)
	u.WriteString(c.addr)
	u.WriteString(r.path)
	for i := 0; i+1 < len(r.params); i += 2 {
		if i > 0 {
			u.WriteByte('&')
		} else {
			u.WriteByte('?')
		}
		u.WriteString(url.QueryEscape(r.params[i]))
		u.WriteByte('=')
		u.WriteString(url.QueryEscape(r.params[i+1]))
	}
	req, err := http.NewRequest(methods[r.method], u.String(), body)
	if err != nil {
		return nil, err
	}
	// req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("X-Consul-Token", c.token)
	}
	return http.DefaultClient.Do(req)
}

func (c *Client) invoke(req *consulRequest) ([]byte, error) {
	resp, err := c.send(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, errorFrom(resp)
	}
	return readBody(resp), nil
}

func (c *Client) query(req *consulRequest, o *QueryOptions, out interface{}) (*QueryMetadata, error) {
	if o != nil {
		if o.LastIndex > 0 {
			req.params = append(req.params, "index", strconv.FormatUint(o.LastIndex, 10))
		}
		if o.WaitTime > 0 {
			req.params = append(req.params, "wait", strconv.FormatInt(o.WaitTime.Milliseconds(), 10)+"ms")
		}
	}
	resp, err := c.send(req)
	if err != nil {
		return nil, err
	}

	meta := &QueryMetadata{}
	meta.LastIndex, _ = strconv.ParseUint(resp.Header.Get("X-Consul-Index"), 10, 64)

	if resp.StatusCode == http.StatusOK {
		if out != nil {
			err = json.Unmarshal(readBody(resp), out)
		} else {
			discardBody(resp)
		}
	} else {
		err = errorFrom(resp)
	}
	return meta, err
}

func NewClient(addr string, token string) *Client {
	addr = strings.Trim(addr, "/")
	if !strings.Contains(addr, "://") {
		addr = "http://" + addr
	}
	return &Client{
		addr:  addr,
		token: token,
	}
}
