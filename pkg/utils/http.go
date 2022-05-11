package utils

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	defaultCallbackTimeout = 5 * time.Second
)

func Post(url string, reader io.Reader) error {
	ctx, _ := context.WithTimeout(context.Background(), defaultCallbackTimeout)
	req, err := http.NewRequestWithContext(ctx, "POST", url, reader)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		if resp.Body != nil {
			resp.Body.Close()
		}

	}()
	if resp.StatusCode >= 400 {
		return fmt.Errorf("http.status=%s", resp.Status)
	}
	return nil
}
