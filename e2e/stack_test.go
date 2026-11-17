package e2e

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/network"
	"github.com/testcontainers/testcontainers-go/wait"
)

const (
	// The server's own test pin. A server that validates ingest with ClickHouse's parser
	// (Wave-RF/WaveHouse#882) needs a line chtypes publishes, which 26.6 is not.
	clickhouseImage = "clickhouse/clickhouse-server:26.8.34.13"
	// The rolling tag of the server's main branch: the SDK follows the server's current protocol.
	defaultServerImage = "ghcr.io/wave-rf/wavehouse:dev"
	// Matches auth.jwt_secret in testdata/settings/config.json.
	jwtSecret = "sdk-e2e-secret" //nolint:gosec // a throwaway secret for the test server, not a credential
)

// stack is the server every test talks to, set up once by TestMain.
var stack struct {
	url        string
	adminToken string
	// managed is true when testcontainers started the server from testdata/settings.
	managed bool
	// clickhouseRows is true for a server that validates ingest with ClickHouse's own
	// parser and sends rows as ClickHouse renders them (Wave-RF/WaveHouse#882), false for
	// a 0.2.x one. Set by probeServer; tests that see the difference assert each side.
	clickhouseRows bool
}

func TestMain(m *testing.M) {
	os.Exit(run(m))
}

func run(m *testing.M) int {
	ctx := context.Background()

	if url := os.Getenv("WAVEHOUSE_URL"); url != "" {
		// An external server: the token must carry the admin role so fixtures can be created.
		stack.url = url
		stack.adminToken = os.Getenv("WAVEHOUSE_ADMIN_TOKEN")
		if stack.adminToken == "" {
			fmt.Fprintln(os.Stderr, "e2e: WAVEHOUSE_URL is set, so WAVEHOUSE_ADMIN_TOKEN must be too")
			return 1
		}
	} else {
		stop, err := startStack(ctx)
		defer stop()
		if err != nil {
			fmt.Fprintf(os.Stderr, "e2e: start stack: %v\n", err)
			return 1
		}
		stack.adminToken = mintToken("admin", 24*time.Hour)
		stack.managed = true
	}

	if err := createFixtures(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "e2e: create fixtures: %v\n", err)
		return 1
	}
	if err := probeServer(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "e2e: probe server: %v\n", err)
		return 1
	}
	fmt.Fprintf(os.Stderr, "e2e: server renders rows with ClickHouse: %v\n", stack.clickhouseRows)
	return m.Run()
}

// startStack runs ClickHouse and the server on a private network. The returned
// stop is safe to call even when startStack fails part-way.
func startStack(ctx context.Context) (stop func(), err error) {
	var cleanups []func()
	stop = func() {
		for i := len(cleanups) - 1; i >= 0; i-- {
			cleanups[i]()
		}
	}

	nw, err := network.New(ctx)
	if err != nil {
		return stop, fmt.Errorf("network: %w", err)
	}
	cleanups = append(cleanups, func() { _ = nw.Remove(context.Background()) })

	ch, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:          clickhouseImage,
			ExposedPorts:   []string{"8123/tcp", "9000/tcp"},
			Networks:       []string{nw.Name},
			NetworkAliases: map[string][]string{nw.Name: {"clickhouse"}},
			Env: map[string]string{
				"CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1",
				"CLICKHOUSE_PASSWORD":                  "",
			},
			// The image declares a VOLUME here; tmpfs keeps it from leaving an anonymous volume behind.
			Tmpfs:      map[string]string{"/var/lib/clickhouse": "rw"},
			WaitingFor: wait.ForHTTP("/ping").WithPort("8123/tcp").WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	cleanups = append(cleanups, func() { _ = testcontainers.TerminateContainer(ch) })
	if err != nil {
		return stop, fmt.Errorf("clickhouse: %w", err)
	}

	image := os.Getenv("WAVEHOUSE_IMAGE")
	if image == "" {
		image = defaultServerImage
	}
	files, err := settingsFiles()
	if err != nil {
		return stop, err
	}
	wh, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: image,
			// :dev moves with the server's main branch, so a cached copy would test an old server.
			AlwaysPullImage: image == defaultServerImage,
			ExposedPorts:    []string{"8080/tcp"},
			Networks:        []string{nw.Name},
			Env:             map[string]string{"WH_DATA_DIR": "/app/data"},
			Files:           files,
			WaitingFor:      wait.ForHTTP("/readyz").WithPort("8080/tcp").WithStartupTimeout(2 * time.Minute),
		},
		Started: true,
	})
	cleanups = append(cleanups, func() { _ = testcontainers.TerminateContainer(wh) })
	if err != nil {
		dumpLogs(wh)
		return stop, fmt.Errorf("wavehouse (%s): %w", image, err)
	}

	endpoint, err := wh.PortEndpoint(ctx, "8080/tcp", "http")
	if err != nil {
		return stop, fmt.Errorf("wavehouse endpoint: %w", err)
	}
	stack.url = endpoint
	return stop, nil
}

func settingsFiles() ([]testcontainers.ContainerFile, error) {
	paths, err := filepath.Glob("testdata/settings/*.json")
	if err != nil || len(paths) == 0 {
		return nil, fmt.Errorf("no settings in testdata/settings: %w", err)
	}
	files := make([]testcontainers.ContainerFile, 0, len(paths))
	for _, p := range paths {
		files = append(files, testcontainers.ContainerFile{
			HostFilePath:      p,
			ContainerFilePath: "/app/settings/" + filepath.Base(p),
			// Copied in as root; the server runs as nonroot and must be able to read them.
			FileMode: 0o644,
		})
	}
	return files, nil
}

func dumpLogs(c testcontainers.Container) {
	if c == nil {
		return
	}
	logs, err := c.Logs(context.Background())
	if err != nil {
		return
	}
	defer func() { _ = logs.Close() }()
	fmt.Fprintln(os.Stderr, "e2e: server logs:")
	_, _ = io.Copy(os.Stderr, logs)
}

// mintToken signs an HS256 JWT with the stack's secret, carrying role in the configured claim.
func mintToken(role string, ttl time.Duration) string {
	enc := base64.RawURLEncoding
	header := enc.EncodeToString([]byte(`{"alg":"HS256","typ":"JWT"}`))
	claims, _ := json.Marshal(map[string]any{
		"sub":  "sdk-e2e",
		"role": role,
		"exp":  time.Now().Add(ttl).Unix(),
	})
	signing := header + "." + enc.EncodeToString(claims)
	mac := hmac.New(sha256.New, []byte(jwtSecret))
	mac.Write([]byte(signing))
	return signing + "." + enc.EncodeToString(mac.Sum(nil))
}

// fixtureDDL creates the tables the tests use. Idempotent, so an external server can be reused.
var fixtureDDL = []string{
	`CREATE TABLE IF NOT EXISTS e2e_events (
		event_id String,
		name     String,
		value    Float64,
		ts       DateTime64(3) DEFAULT now64(3)
	) ENGINE = MergeTree ORDER BY (name, ts)`,
	// 64-bit integers past 2^53 and a Decimal: a JSON string from a 0.2.x server, a number from Wave-RF/WaveHouse#882 on.
	`CREATE TABLE IF NOT EXISTS e2e_numbers (
		event_id String,
		big      UInt64,
		signed   Int64,
		price    Decimal(38, 10)
	) ENGINE = MergeTree ORDER BY event_id`,
}

// createFixtures goes through the server's admin endpoints directly rather than the SDK,
// so a broken SDK fails its own tests instead of the whole run.
func createFixtures(ctx context.Context) error {
	for _, ddl := range fixtureDDL {
		if err := adminCall(ctx, http.MethodPost, "/v1/ops/query", map[string]string{"sql": ddl}); err != nil {
			return err
		}
	}
	if err := adminCall(ctx, http.MethodPost, "/v1/ops/schema/refresh", nil); err != nil {
		return err
	}
	// The refresh is synchronous on one instance; poll anyway so a lagging server is not a flake.
	deadline := time.Now().Add(30 * time.Second)
	for {
		err := adminCall(ctx, http.MethodGet, "/v1/ops/schema?table=e2e_events", nil)
		if err == nil {
			err = adminCall(ctx, http.MethodGet, "/v1/ops/schema?table=e2e_numbers", nil)
		}
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("the fixture tables never appeared in the schema: %w", err)
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func adminCall(ctx context.Context, method, path string, body any) error {
	var rd io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, stack.url+path, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+stack.adminToken)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = res.Body.Close() }()
	if res.StatusCode/100 != 2 {
		msg, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
		return fmt.Errorf("%s %s: %d %s", method, path, res.StatusCode, bytes.TrimSpace(msg))
	}
	return nil
}

// probeServer sets stack.clickhouseRows from how the server answers one record naming a
// column e2e_events lacks: refused whole with ClickHouse's code 117 by a server that parses
// ingest with ClickHouse (Wave-RF/WaveHouse#882), rejected on its own in a 200 by a 0.2.x
// one. Neither publishes anything. Any other answer fails the run.
func probeServer(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, stack.url+"/v1/ingest?table=e2e_events",
		bytes.NewReader([]byte(`{"event_id":"e2e-probe","no_such_column":1}`)))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+stack.adminToken)
	req.Header.Set("Content-Type", "application/x-ndjson")
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = res.Body.Close() }()
	raw, _ := io.ReadAll(io.LimitReader(res.Body, 1<<16))
	var body struct {
		Code          string `json:"code"`
		ExceptionCode int    `json:"exception_code"`
		Failed        int    `json:"failed"`
	}
	_ = json.Unmarshal(raw, &body)
	switch {
	case res.StatusCode == http.StatusBadRequest && body.Code == "clickhouse.rejected" && body.ExceptionCode == 117:
		stack.clickhouseRows = true
	case res.StatusCode == http.StatusOK && body.Failed == 1:
		stack.clickhouseRows = false
	default:
		return fmt.Errorf("a record with an unknown column: %d %s", res.StatusCode, bytes.TrimSpace(raw))
	}
	return nil
}
