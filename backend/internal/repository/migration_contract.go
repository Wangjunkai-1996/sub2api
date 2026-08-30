package repository

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"sort"
	"strings"

	"github.com/Wei-Shaw/sub2api/migrations"
)

// MigrationContract is a read-only description of the migrations embedded in
// a release binary. Deployment tooling uses it before starting a candidate.
type MigrationContract struct {
	FormatVersion int                      `json:"format_version"`
	Migrations    []MigrationContractEntry `json:"migrations"`
}

// MigrationContractEntry lists the database checksums accepted by this exact
// binary for one embedded migration file.
type MigrationContractEntry struct {
	Name                      string   `json:"name"`
	Checksum                  string   `json:"checksum"`
	AcceptedDatabaseChecksums []string `json:"accepted_database_checksums"`
}

// EmbeddedMigrationContract returns the contract for the migration set built
// into the current binary. It never connects to or modifies a database.
func EmbeddedMigrationContract() (MigrationContract, error) {
	return migrationContract(migrations.FS)
}

func migrationContract(fsys fs.FS) (MigrationContract, error) {
	names, err := fs.Glob(fsys, "*.sql")
	if err != nil {
		return MigrationContract{}, fmt.Errorf("list migrations: %w", err)
	}
	sort.Strings(names)

	contract := MigrationContract{FormatVersion: 1}
	for _, name := range names {
		contentBytes, err := fs.ReadFile(fsys, name)
		if err != nil {
			return MigrationContract{}, fmt.Errorf("read migration %s: %w", name, err)
		}
		content := strings.TrimSpace(string(contentBytes))
		if content == "" {
			continue
		}
		sum := sha256.Sum256([]byte(content))
		checksum := hex.EncodeToString(sum[:])
		accepted := []string{checksum}
		if rule, ok := migrationChecksumCompatibilityRules[name]; ok {
			if _, recognized := rule.acceptedChecksums[checksum]; recognized {
				accepted = make([]string, 0, len(rule.acceptedChecksums))
				for value := range rule.acceptedChecksums {
					accepted = append(accepted, value)
				}
				sort.Strings(accepted)
			}
		}
		contract.Migrations = append(contract.Migrations, MigrationContractEntry{
			Name:                      name,
			Checksum:                  checksum,
			AcceptedDatabaseChecksums: accepted,
		})
	}
	return contract, nil
}
