package migrations

import (
	"crypto/sha256"
	"fmt"
	"io/fs"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEnableGeminiImageGenerationGroupsMigration(t *testing.T) {
	content, err := FS.ReadFile("255_enable_gemini_image_generation_groups.sql")
	require.NoError(t, err)

	var statements []string
	for _, line := range strings.Split(string(content), "\n") {
		if !strings.HasPrefix(strings.TrimSpace(line), "--") {
			statements = append(statements, line)
		}
	}
	sql := strings.Join(strings.Fields(strings.Join(statements, " ")), " ")
	require.Equal(t,
		"UPDATE groups SET allow_image_generation = true WHERE platform IN ('gemini', 'antigravity') AND allow_image_generation = false;",
		sql)
}

func TestV0218MigrationPreservesKKAILedger(t *testing.T) {
	// Freeze the names and bytes from release/v0.2.17-kkai at 3478a5458.
	// Reintroducing an upstream name for an already-renumbered migration would
	// rerun it against an existing database. v0.2.20 adds three new files,
	// v0.2.21 adds two, and v0.2.22 adds two; their interleaved names are
	// outside this frozen baseline.
	names, err := fs.Glob(FS, "*.sql")
	require.NoError(t, err)
	legacy := sha256.New()
	for _, name := range names {
		if name >= "255_" {
			continue
		}
		switch name {
		case "247_scoped_admin_api_keys.sql", "248_group_image_count_billing.sql", "249_mcp_oauth.sql",
			"249_add_usage_logs_api_key_cost_index_notx.sql", "250_openai_cache_write_ttl.sql",
			"251_group_force_openai_ultrafast.sql", "251_group_usage_rollup_nonblocking_build.sql":
			continue
		}
		content, err := FS.ReadFile(name)
		require.NoError(t, err)
		fmt.Fprintf(legacy, "%s\x00%s\x00", name, content)
	}
	require.Equal(t, "2386c4c4ec38443465f32ae851a7a5606582956d5f09e3a3fede6b7499b3ddbf", fmt.Sprintf("%x", legacy.Sum(nil)),
		"previous migration names and contents must remain immutable")
}
