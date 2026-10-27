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
	// rerun it against an existing database. New migrations start at 255.
	names, err := fs.Glob(FS, "*.sql")
	require.NoError(t, err)
	legacy := sha256.New()
	for _, name := range names {
		if name >= "255_" {
			continue
		}
		content, err := FS.ReadFile(name)
		require.NoError(t, err)
		fmt.Fprintf(legacy, "%s\x00%s\x00", name, content)
	}
	require.Equal(t, "2386c4c4ec38443465f32ae851a7a5606582956d5f09e3a3fede6b7499b3ddbf", fmt.Sprintf("%x", legacy.Sum(nil)),
		"previous migration names and contents must remain immutable")
}
