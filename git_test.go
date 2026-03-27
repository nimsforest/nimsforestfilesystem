package nimsforestfilesystem

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/nimsforest/nimsforest2/pkg/bedrock"
)

func TestGitBedrock_Lifecycle(t *testing.T) {
	tempDirectory := t.TempDir()

	gb := NewGitBedrock("test", tempDirectory, nil)

	if err := gb.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer gb.Stop()

	if gb.Name() != "test" {
		t.Errorf("expected name 'test', got %q", gb.Name())
	}
	if gb.Type() != "git" {
		t.Errorf("expected type 'git', got %q", gb.Type())
	}
	if gb.IsReadOnly() {
		t.Error("expected not read-only")
	}

	// Verify git repo was initialized
	gitDirectory := filepath.Join(tempDirectory, ".git")
	if _, err := os.Stat(gitDirectory); os.IsNotExist(err) {
		t.Fatal(".git directory not created")
	}
}

func TestGitBedrock_WriteAndRead(t *testing.T) {
	tempDirectory := t.TempDir()
	gb := NewGitBedrock("test", tempDirectory, nil)
	if err := gb.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer gb.Stop()

	content := []byte("# Hello World\n\nThis is a test document.\n")
	err := gb.Write("docs/hello.md", content,
		bedrock.WithAuthor("tester"),
		bedrock.WithMessage("add hello doc"),
	)
	if err != nil {
		t.Fatalf("Write failed: %v", err)
	}

	// Read it back
	data, err := gb.Read("docs/hello.md")
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if string(data) != string(content) {
		t.Errorf("content mismatch: got %q", string(data))
	}

	// Stat it
	fi, err := gb.Stat("docs/hello.md")
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if fi.FileName != "hello.md" {
		t.Errorf("expected file name 'hello.md', got %q", fi.FileName)
	}
	if fi.IsDirectory {
		t.Error("expected not directory")
	}
	if fi.Size != int64(len(content)) {
		t.Errorf("expected size %d, got %d", len(content), fi.Size)
	}
}

func TestGitBedrock_List(t *testing.T) {
	tempDirectory := t.TempDir()
	gb := NewGitBedrock("test", tempDirectory, nil)
	if err := gb.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer gb.Stop()

	// Create some files
	gb.Write("alpha.md", []byte("# Alpha"))
	gb.Write("beta.md", []byte("# Beta"))
	gb.Write("subdir/gamma.md", []byte("# Gamma"))

	// List root
	entries, err := gb.List("")
	if err != nil {
		t.Fatalf("List root failed: %v", err)
	}

	names := make(map[string]bool)
	for _, e := range entries {
		names[e.FileName] = true
	}

	if !names["alpha.md"] {
		t.Error("expected alpha.md in root listing")
	}
	if !names["beta.md"] {
		t.Error("expected beta.md in root listing")
	}
	if !names["subdir"] {
		t.Error("expected subdir in root listing")
	}

	// List subdirectory
	subEntries, err := gb.List("subdir")
	if err != nil {
		t.Fatalf("List subdir failed: %v", err)
	}
	if len(subEntries) != 1 || subEntries[0].FileName != "gamma.md" {
		t.Errorf("expected gamma.md in subdir listing, got %v", subEntries)
	}
}

func TestGitBedrock_Delete(t *testing.T) {
	tempDirectory := t.TempDir()
	gb := NewGitBedrock("test", tempDirectory, nil)
	if err := gb.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer gb.Stop()

	gb.Write("to-delete.md", []byte("# Delete Me"))

	err := gb.Delete("to-delete.md", bedrock.WithAuthor("tester"))
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify file is gone
	_, err = gb.Read("to-delete.md")
	if err == nil {
		t.Error("expected error reading deleted file")
	}
}

func TestGitBedrock_WriteUpdate(t *testing.T) {
	tempDirectory := t.TempDir()
	gb := NewGitBedrock("test", tempDirectory, nil)
	if err := gb.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer gb.Stop()

	gb.Write("doc.md", []byte("version 1"))
	gb.Write("doc.md", []byte("version 2"))

	data, err := gb.Read("doc.md")
	if err != nil {
		t.Fatalf("Read failed: %v", err)
	}
	if string(data) != "version 2" {
		t.Errorf("expected 'version 2', got %q", string(data))
	}
}

func TestGitBedrock_PathTraversal(t *testing.T) {
	tempDirectory := t.TempDir()
	gb := NewGitBedrock("test", tempDirectory, nil)
	if err := gb.Start(context.Background()); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer gb.Stop()

	_, err := gb.Read("../../etc/passwd")
	if err == nil {
		t.Error("expected path traversal error")
	}

	err = gb.Write("../../etc/evil", []byte("bad"))
	if err == nil {
		t.Error("expected path traversal error on write")
	}
}
