package workspace

import (
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestWorkspaceModules(t *testing.T) {
	t.Parallel()

	root, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}

	modules := []string{
		"services/order-service",
		"services/matching-engine",
		"services/portfolio-service",
		"services/websocket-service",
	}

	for _, modulePath := range modules {
		modulePath := modulePath
		t.Run(modulePath, func(t *testing.T) {
			t.Parallel()

			cmd := exec.Command("go", "test", "./...")
			cmd.Dir = filepath.Join(root, modulePath)
			cmd.Env = append(os.Environ(), "GOCACHE=/tmp/tradesphere-go-cache-"+filepath.Base(modulePath))
			output, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("go test failed for %s: %v\n%s", modulePath, err, string(output))
			}
		})
	}
}

func TestE2E(t *testing.T) {
	if os.Getenv("E2E") != "1" {
		t.Skip("Skipping E2E test. Set E2E=1 to run.")
	}

	root, err := os.Getwd()
	if err != nil {
		t.Fatalf("get working directory: %v", err)
	}

	cmd := exec.Command("./scripts/e2e_test.sh")
	cmd.Dir = root
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		t.Fatalf("E2E test failed: %v", err)
	}
}
