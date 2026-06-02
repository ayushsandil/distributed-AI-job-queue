package postgres

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestMigrationFilesReturnsSortedSQLFiles(t *testing.T) {
	dir := t.TempDir()
	writeTestFile(t, filepath.Join(dir, "002_second.sql"))
	writeTestFile(t, filepath.Join(dir, "notes.txt"))
	writeTestFile(t, filepath.Join(dir, "001_first.sql"))

	files, err := migrationFiles(dir)
	if err != nil {
		t.Fatalf("migrationFiles returned error: %v", err)
	}

	want := []string{
		filepath.Join(dir, "001_first.sql"),
		filepath.Join(dir, "002_second.sql"),
	}
	if !reflect.DeepEqual(files, want) {
		t.Fatalf("expected %v, got %v", want, files)
	}
}

func writeTestFile(t *testing.T, path string) {
	t.Helper()

	if err := os.WriteFile(path, []byte("-- test\n"), 0o644); err != nil {
		t.Fatalf("write test file: %v", err)
	}
}
