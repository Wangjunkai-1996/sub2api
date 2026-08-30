package repository

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
)

func TestMigrationContractUsesRunnerChecksums(t *testing.T) {
	contract, err := migrationContract(fstest.MapFS{
		"002_second.sql": {Data: []byte("\n SELECT 2; \n")},
		"001_first.sql":  {Data: []byte("SELECT 1;\n")},
		"empty.sql":      {Data: nil},
	})
	require.NoError(t, err)
	require.Equal(t, 1, contract.FormatVersion)
	require.Len(t, contract.Migrations, 2)
	require.Equal(t, "001_first.sql", contract.Migrations[0].Name)

	sum := sha256.Sum256([]byte("SELECT 1;"))
	want := hex.EncodeToString(sum[:])
	require.Equal(t, want, contract.Migrations[0].Checksum)
	require.Equal(t, []string{want}, contract.Migrations[0].AcceptedDatabaseChecksums)
}

func TestEmbeddedMigrationContractExposesKnownCompatibility(t *testing.T) {
	contract, err := EmbeddedMigrationContract()
	require.NoError(t, err)

	var migration MigrationContractEntry
	for _, entry := range contract.Migrations {
		if entry.Name == "054_drop_legacy_cache_columns.sql" {
			migration = entry
			break
		}
	}
	require.Equal(t, "054_drop_legacy_cache_columns.sql", migration.Name)
	require.Contains(t, migration.AcceptedDatabaseChecksums,
		"182c193f3359946cf094090cd9e57d5c3fd9abaffbc1e8fc378646b8a6fa12b4")
	require.Contains(t, migration.AcceptedDatabaseChecksums, migration.Checksum)
}
