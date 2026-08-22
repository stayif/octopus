package dockercontract

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func repositoryRoot(t *testing.T) string {
	t.Helper()
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(filename), "..", ".."))
}

func TestPublishedDockerfilesInstallComposeHealthcheckTool(t *testing.T) {
	root := repositoryRoot(t)
	cases := []struct {
		name string
		path string
		line string
	}{
		{
			name: "alpine",
			path: filepath.Join(root, "scripts", "dockerfiles", "Dockerfile.alpine"),
			line: "apk add --no-cache alpine-conf ca-certificates su-exec wget",
		},
		{
			name: "debian",
			path: filepath.Join(root, "scripts", "dockerfiles", "Dockerfile.debian"),
			line: "apt-get install -y ca-certificates tzdata gosu wget",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			contents, err := os.ReadFile(tc.path)
			if err != nil {
				t.Fatal(err)
			}
			text := string(contents)
			if !strings.Contains(text, tc.line) {
				t.Fatalf("Dockerfile does not install the Compose healthcheck tool: missing %q", tc.line)
			}
			if !strings.Contains(text, "COPY build/docker/${TARGETPLATFORM}/octopus /app/octopus") {
				t.Fatal("Dockerfile does not copy the target-platform Octopus binary")
			}
		})
	}
}
