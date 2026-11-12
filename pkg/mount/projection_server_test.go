package mount

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Evref-BL/pharo-image-fs-daemon/pkg/protocol"
)

// projectionHandler serves the projection protocol from client, as the
// image does, for tests that run the daemon itself.
func projectionHandler(client protocol.Client) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var body struct {
			Path       string `json:"path"`
			Text       string `json:"text"`
			TargetPath string `json:"targetPath"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		ctx := request.Context()
		var answer any
		var err error
		switch operation := request.URL.Path[strings.LastIndex(request.URL.Path, "/")+1:]; operation {
		case "list":
			var entries []protocol.Entry
			entries, err = client.List(ctx, body.Path)
			answer = map[string]any{"entries": entries}
		case "stat":
			var entry protocol.Entry
			entry, err = client.Stat(ctx, body.Path)
			answer = map[string]any{"entry": entry}
		case "read":
			var contents []byte
			contents, err = client.Read(ctx, body.Path)
			answer = map[string]any{"text": string(contents)}
		case "write":
			answer, err = client.Write(ctx, body.Path, []byte(body.Text))
		case "delete":
			err = client.Delete(ctx, body.Path)
		case "rename":
			err = client.Rename(ctx, body.Path, body.TargetPath)
		case "mkdir":
			err = client.Mkdir(ctx, body.Path)
		case "rmdir":
			err = client.Rmdir(ctx, body.Path)
		default:
			http.NotFound(writer, request)
			return
		}
		writer.Header().Set("Content-Type", "application/json")
		var refused *protocol.Error
		switch {
		case errors.As(err, &refused):
			writer.WriteHeader(refused.StatusCode)
			_ = json.NewEncoder(writer).Encode(map[string]string{"message": refused.Message})
		case err != nil:
			writer.WriteHeader(http.StatusInternalServerError)
			_ = json.NewEncoder(writer).Encode(map[string]string{"message": err.Error()})
		case answer == nil:
			_, _ = writer.Write([]byte("{}"))
		default:
			_ = json.NewEncoder(writer).Encode(answer)
		}
	})
}

func TestTheProjectionHandlerSpeaksTheProtocol(t *testing.T) {
	image := newMemoryClient(map[string]string{objectPath: "source"})
	server := httptest.NewServer(projectionHandler(image))
	defer server.Close()
	client, err := protocol.NewHTTPClient(server.URL + "/projection")
	if err != nil {
		t.Fatalf("client: %v", err)
	}
	ctx := context.Background()
	if entries, err := client.List(ctx, "/tonel/Kernel"); err != nil || len(entries) != 1 || entries[0].Name != "Object.class.st" {
		t.Fatalf("list: %v %v", entries, err)
	}
	if contents, err := client.Read(ctx, objectPath); err != nil || string(contents) != "source" {
		t.Fatalf("read: %q %v", contents, err)
	}
	if _, err := client.Write(ctx, objectPath, []byte("syntax error")); !errors.Is(errnoFor(err), errnoFor(&protocol.Error{StatusCode: http.StatusBadRequest})) {
		t.Fatalf("a refusal should come back as one, got %v", err)
	}
	if _, err := client.Stat(ctx, "/tonel/Kernel/Missing.class.st"); !protocol.NotFound(err) {
		t.Fatalf("a missing file should come back as not found, got %v", err)
	}
}
