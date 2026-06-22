package push

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"golang.org/x/oauth2"
)

func newTestFCMProvider(endpoint string) *FCMProvider {
	return &FCMProvider{
		projectID:   "test-project",
		tokenSource: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "test-access-token"}),
		httpClient:  http.DefaultClient,
		endpoint:    endpoint,
		l:           slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func TestFCMSendIsHybridNotificationPlusData(t *testing.T) {
	var gotPath, gotAuth string
	var gotBody map[string]json.RawMessage
	var gotMessage struct {
		Token        string            `json:"token"`
		Data         map[string]string `json:"data"`
		Notification *struct {
			Title string `json:"title"`
			Body  string `json:"body"`
		} `json:"notification"`
		Android *struct {
			Priority     string `json:"priority"`
			Notification *struct {
				ChannelID string `json:"channel_id"`
			} `json:"notification"`
		} `json:"android"`
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		_ = json.Unmarshal(gotBody["message"], &gotMessage)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"name":"projects/test-project/messages/123"}`))
	}))
	defer srv.Close()

	p := newTestFCMProvider(srv.URL)
	err := p.Send(context.Background(), "device-fcm-token", Notification{
		Title:     "Turn complete",
		Body:      "Your agent finished.",
		Category:  CategoryTurnComplete,
		ServerID:  "gw-1",
		SessionID: "sess-1",
	})
	if err != nil {
		t.Fatalf("Send() error = %v", err)
	}

	if want := "/v1/projects/test-project/messages:send"; gotPath != want {
		t.Errorf("request path = %q, want %q", gotPath, want)
	}
	if want := "Bearer test-access-token"; gotAuth != want {
		t.Errorf("auth header = %q, want %q", gotAuth, want)
	}
	if gotMessage.Token != "device-fcm-token" {
		t.Errorf("message token = %q, want %q", gotMessage.Token, "device-fcm-token")
	}
	// The notification block must be present so a killed app's system can display
	// the alert with no app process running.
	if gotMessage.Notification == nil {
		t.Fatal("notification block absent, want present (killed app needs it to display)")
	}
	if gotMessage.Notification.Title != "Turn complete" {
		t.Errorf("notification title = %q, want %q", gotMessage.Notification.Title, "Turn complete")
	}
	if gotMessage.Notification.Body != "Your agent finished." {
		t.Errorf("notification body = %q, want %q", gotMessage.Notification.Body, "Your agent finished.")
	}
	// title/body are duplicated into data so the foreground client renders + routes.
	if gotMessage.Data["title"] != "Turn complete" {
		t.Errorf("data title = %q, want %q", gotMessage.Data["title"], "Turn complete")
	}
	if gotMessage.Data["serverId"] != "gw-1" {
		t.Errorf("data serverId = %q, want %q", gotMessage.Data["serverId"], "gw-1")
	}
	if gotMessage.Data["sessionId"] != "sess-1" {
		t.Errorf("data sessionId = %q, want %q", gotMessage.Data["sessionId"], "sess-1")
	}
	// Android delivery: high priority and the quiet Updates channel for turn-complete.
	if gotMessage.Android == nil {
		t.Fatal("android block absent, want present")
	}
	if gotMessage.Android.Priority != "high" {
		t.Errorf("android priority = %q, want %q", gotMessage.Android.Priority, "high")
	}
	if gotMessage.Android.Notification == nil || gotMessage.Android.Notification.ChannelID != updatesChannelID {
		t.Errorf("android channel_id = %+v, want %q", gotMessage.Android.Notification, updatesChannelID)
	}
}

func TestFCMSendRoutesCategoriesToChannels(t *testing.T) {
	cases := []struct {
		category    string
		wantChannel string
	}{
		{CategoryTurnComplete, updatesChannelID},
		{CategoryPermissionRequest, alertChannelID},
		{CategoryError, alertChannelID},
		{CategoryAgentCrash, alertChannelID},
		{"", updatesChannelID}, // unknown/empty category defaults to the quiet channel
	}
	for _, tc := range cases {
		t.Run(tc.category, func(t *testing.T) {
			var gotMessage struct {
				Android struct {
					Notification struct {
						ChannelID string `json:"channel_id"`
					} `json:"notification"`
				} `json:"android"`
			}
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var body map[string]json.RawMessage
				_ = json.NewDecoder(r.Body).Decode(&body)
				_ = json.Unmarshal(body["message"], &gotMessage)
				w.WriteHeader(http.StatusOK)
			}))
			defer srv.Close()

			p := newTestFCMProvider(srv.URL)
			if err := p.Send(context.Background(), "tok", Notification{Title: "T", Category: tc.category}); err != nil {
				t.Fatalf("Send() error = %v", err)
			}
			if got := gotMessage.Android.Notification.ChannelID; got != tc.wantChannel {
				t.Errorf("category %q channel_id = %q, want %q", tc.category, got, tc.wantChannel)
			}
		})
	}
}

func TestFCMSendOmitsEmptyDataFields(t *testing.T) {
	var gotMessage struct {
		Data map[string]string `json:"data"`
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body map[string]json.RawMessage
		_ = json.NewDecoder(r.Body).Decode(&body)
		_ = json.Unmarshal(body["message"], &gotMessage)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	p := newTestFCMProvider(srv.URL)
	if err := p.Send(context.Background(), "tok", Notification{Title: "T", Body: "B"}); err != nil {
		t.Fatalf("Send() error = %v", err)
	}
	if _, ok := gotMessage.Data["serverId"]; ok {
		t.Error("empty serverId should be omitted from data")
	}
	if _, ok := gotMessage.Data["cwd"]; ok {
		t.Error("empty cwd should be omitted from data")
	}
}

func TestFCMSendReportsUnregisteredOn404(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"error":{"code":404,"status":"NOT_FOUND","details":[{"errorCode":"UNREGISTERED"}]}}`))
	}))
	defer srv.Close()

	p := newTestFCMProvider(srv.URL)
	err := p.Send(context.Background(), "dead-token", Notification{Title: "T"})
	if !errors.Is(err, ErrTokenUnregistered) {
		t.Fatalf("Send() error = %v, want ErrTokenUnregistered", err)
	}
}

func TestFCMSendReportsUnregisteredOnErrorCodeWithout404(t *testing.T) {
	// FCM can return UNREGISTERED inside a 400 body too; the error code, not the
	// HTTP status, is authoritative for token death.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"error":{"code":400,"status":"INVALID_ARGUMENT","details":[{"errorCode":"UNREGISTERED"}]}}`))
	}))
	defer srv.Close()

	p := newTestFCMProvider(srv.URL)
	if err := p.Send(context.Background(), "dead-token", Notification{Title: "T"}); !errors.Is(err, ErrTokenUnregistered) {
		t.Fatalf("Send() error = %v, want ErrTokenUnregistered", err)
	}
}

func TestFCMSendReturnsErrorOnServerFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":{"code":500,"status":"INTERNAL"}}`))
	}))
	defer srv.Close()

	p := newTestFCMProvider(srv.URL)
	err := p.Send(context.Background(), "live-token", Notification{Title: "T"})
	if err == nil {
		t.Fatal("expected error on 500 response, got nil")
	}
	if errors.Is(err, ErrTokenUnregistered) {
		t.Error("a transient 500 must not be reported as token death")
	}
}
