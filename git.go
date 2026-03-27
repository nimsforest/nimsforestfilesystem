package nimsforestfilesystem

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"mime"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"github.com/nimsforest/nimsforest2/pkg/bedrock"
	"github.com/nimsforest/nimsforest2/pkg/nim"
)

// GitBedrock is a persistent storage implementation backed by a local git repository.
// Every Write and Delete operation creates a git commit, providing a full audit trail.
// It emits bedrock events on Wind so the ReadTreehouse can index contents into Soil.
type GitBedrock struct {
	name     string
	path     string
	readOnly bool
	wind     *nim.Wind

	mu sync.RWMutex
}

// NewGitBedrock creates a new git-backed bedrock instance.
// The path must be a local directory. If it is not already a git repo, Start will initialize one.
func NewGitBedrock(name, path string, wind *nim.Wind) *GitBedrock {
	return &GitBedrock{
		name: name,
		path: path,
		wind: wind,
	}
}

// SetWind sets the Wind instance for emitting bedrock events.
// Call this after NATS connects if Wind was nil at construction time.
func (g *GitBedrock) SetWind(wind *nim.Wind) {
	g.wind = wind
}

// Start initializes the git repository if needed and emits a mounted event.
func (g *GitBedrock) Start(ctx context.Context) error {
	if err := os.MkdirAll(g.path, 0755); err != nil {
		return fmt.Errorf("creating bedrock directory: %w", err)
	}

	gitDirectory := filepath.Join(g.path, ".git")
	if _, err := os.Stat(gitDirectory); os.IsNotExist(err) {
		if err := g.gitInit(); err != nil {
			return fmt.Errorf("git init: %w", err)
		}
	}

	log.Printf("[Bedrock:%s] Started (type=git, path=%s)", g.name, g.path)

	if g.wind != nil {
		event := bedrock.MountedEvent{
			BedrockName: g.name,
			BedrockType: "git",
			ReadOnly:    g.readOnly,
		}
		data, _ := json.Marshal(event)
		leaf := nim.NewLeaf(bedrock.Subject(g.name, bedrock.EventMounted), data, "bedrock."+g.name)
		if err := g.wind.Drop(*leaf); err != nil {
			log.Printf("[Bedrock:%s] Failed to emit mounted event: %v", g.name, err)
		}
	}

	return nil
}

// Stop gracefully shuts down the bedrock.
func (g *GitBedrock) Stop() error {
	log.Printf("[Bedrock:%s] Stopped", g.name)
	return nil
}

// Name returns the configured name of this bedrock instance.
func (g *GitBedrock) Name() string { return g.name }

// Type returns "git".
func (g *GitBedrock) Type() string { return "git" }

// IsReadOnly returns whether this bedrock supports writes.
func (g *GitBedrock) IsReadOnly() bool { return g.readOnly }

// List returns file information for entries in the given directory path.
func (g *GitBedrock) List(path string) ([]bedrock.FileInfo, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	absPath := g.resolve(path)
	if !g.isWithinRoot(absPath) {
		return nil, fmt.Errorf("path traversal not allowed: %s", path)
	}

	entries, err := os.ReadDir(absPath)
	if err != nil {
		return nil, err
	}

	var result []bedrock.FileInfo
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".") {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		entryPath := path
		if entryPath == "" || entryPath == "/" {
			entryPath = entry.Name()
		} else {
			entryPath = filepath.Join(path, entry.Name())
		}
		entryAbsPath := filepath.Join(absPath, entry.Name())
		result = append(result, fileInfoFromOS(entryPath, entryAbsPath, info))
	}

	return result, nil
}

// Read returns the content of a file at the given path.
func (g *GitBedrock) Read(path string) ([]byte, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	absPath := g.resolve(path)
	if !g.isWithinRoot(absPath) {
		return nil, fmt.Errorf("path traversal not allowed: %s", path)
	}

	return os.ReadFile(absPath)
}

// Stat returns metadata about a single file or directory.
func (g *GitBedrock) Stat(path string) (*bedrock.FileInfo, error) {
	g.mu.RLock()
	defer g.mu.RUnlock()

	absPath := g.resolve(path)
	if !g.isWithinRoot(absPath) {
		return nil, fmt.Errorf("path traversal not allowed: %s", path)
	}

	info, err := os.Stat(absPath)
	if err != nil {
		return nil, err
	}

	fi := fileInfoFromOS(path, absPath, info)
	return &fi, nil
}

// Write creates or updates a file and commits the change.
func (g *GitBedrock) Write(path string, content []byte, options ...bedrock.WriteOption) error {
	if g.readOnly {
		return bedrock.ErrReadOnly{BedrockName: g.name}
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	absPath := g.resolve(path)
	if !g.isWithinRoot(absPath) {
		return fmt.Errorf("path traversal not allowed: %s", path)
	}

	config := bedrock.ApplyWriteOptions(options...)

	// Determine if this is a create or update
	_, err := os.Stat(absPath)
	isCreate := os.IsNotExist(err)

	// Ensure parent directory exists
	if err := os.MkdirAll(filepath.Dir(absPath), 0755); err != nil {
		return fmt.Errorf("creating parent directory: %w", err)
	}

	if err := os.WriteFile(absPath, content, 0644); err != nil {
		return fmt.Errorf("writing file: %w", err)
	}

	// Build commit message
	action := "update"
	if isCreate {
		action = "create"
	}
	commitMessage := g.buildCommitMessage(action, path, config)

	// Git add and commit
	gitPath := strings.ReplaceAll(path, string(filepath.Separator), "/")
	if err := g.gitExec("add", gitPath); err != nil {
		return fmt.Errorf("git add: %w", err)
	}
	if err := g.gitExec("commit", "-m", commitMessage); err != nil {
		return fmt.Errorf("git commit: %w", err)
	}

	// Emit event
	eventType := bedrock.EventFileModified
	if isCreate {
		eventType = bedrock.EventFileCreated
	}
	g.emitFileEvent(eventType, path)

	return nil
}

// Delete removes a file and commits the deletion.
func (g *GitBedrock) Delete(path string, options ...bedrock.WriteOption) error {
	if g.readOnly {
		return bedrock.ErrReadOnly{BedrockName: g.name}
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	absPath := g.resolve(path)
	if !g.isWithinRoot(absPath) {
		return fmt.Errorf("path traversal not allowed: %s", path)
	}

	if _, err := os.Stat(absPath); os.IsNotExist(err) {
		return fmt.Errorf("file does not exist: %s", path)
	}

	if err := os.Remove(absPath); err != nil {
		return fmt.Errorf("removing file: %w", err)
	}

	config := bedrock.ApplyWriteOptions(options...)
	commitMessage := g.buildCommitMessage("delete", path, config)

	if err := g.gitExec("add", "-A"); err != nil {
		return fmt.Errorf("git add: %w", err)
	}
	if err := g.gitExec("commit", "-m", commitMessage); err != nil {
		return fmt.Errorf("git commit: %w", err)
	}

	g.emitFileEvent(bedrock.EventFileDeleted, path)

	return nil
}

// resolve converts a relative bedrock path to an absolute filesystem path.
func (g *GitBedrock) resolve(path string) string {
	if path == "" || path == "/" {
		return g.path
	}
	return filepath.Join(g.path, filepath.FromSlash(path))
}

// isWithinRoot validates that an absolute path is within the bedrock root.
func (g *GitBedrock) isWithinRoot(absPath string) bool {
	cleaned := filepath.Clean(absPath)
	return strings.HasPrefix(cleaned, g.path)
}

// gitInit initializes a new git repository at the bedrock path.
func (g *GitBedrock) gitInit() error {
	commands := [][]string{
		{"init"},
		{"config", "user.name", "nimsforestfilesystem"},
		{"config", "user.email", "filesystem@nimsforest.com"},
	}
	for _, args := range commands {
		if err := g.gitExec(args...); err != nil {
			return err
		}
	}

	// Create initial commit with .gitkeep
	gitkeep := filepath.Join(g.path, ".gitkeep")
	if err := os.WriteFile(gitkeep, []byte{}, 0644); err != nil {
		return err
	}
	if err := g.gitExec("add", "-A"); err != nil {
		return err
	}
	return g.gitExec("commit", "-m", "initial commit")
}

// gitExec runs a git command in the bedrock directory.
func (g *GitBedrock) gitExec(args ...string) error {
	cmd := exec.Command("git", args...)
	cmd.Dir = g.path
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("git %v: %s: %w", args, string(out), err)
	}
	return nil
}

// buildCommitMessage creates a structured commit message from the action and options.
func (g *GitBedrock) buildCommitMessage(action, path string, config bedrock.WriteConfig) string {
	if config.Message != "" {
		msg := config.Message
		if config.Author != "" {
			msg += "\n\nAuthor: " + config.Author
		}
		return msg
	}
	msg := fmt.Sprintf("%s %s", action, path)
	if config.Author != "" {
		msg += "\n\nAuthor: " + config.Author
	}
	return msg
}

// emitFileEvent publishes a bedrock file event on Wind.
func (g *GitBedrock) emitFileEvent(eventType, path string) {
	if g.wind == nil {
		return
	}

	absPath := g.resolve(path)
	info, err := os.Stat(absPath)

	var fi bedrock.FileInfo
	if err == nil {
		fi = fileInfoFromOS(path, absPath, info)
	} else {
		// File was deleted — construct minimal FileInfo
		fi = bedrock.FileInfo{
			Path:     path,
			FileName: filepath.Base(path),
		}
	}

	event := bedrock.FileEvent{
		BedrockName: g.name,
		BedrockType: "git",
		File:        fi,
	}
	data, _ := json.Marshal(event)
	subject := bedrock.Subject(g.name, eventType)
	leaf := nim.NewLeaf(subject, data, "bedrock."+g.name)
	if err := g.wind.Drop(*leaf); err != nil {
		log.Printf("[Bedrock:%s] Failed to emit %s event for %s: %v", g.name, eventType, path, err)
	}
}

// fileInfoFromOS converts an os.FileInfo to a bedrock.FileInfo.
// relPath is the bedrock-relative path, absPath is the absolute filesystem path.
func fileInfoFromOS(relPath, absPath string, info os.FileInfo) bedrock.FileInfo {
	fi := bedrock.FileInfo{
		Path:         relPath,
		FileName:     info.Name(),
		Size:         info.Size(),
		IsDirectory:  info.IsDir(),
		ModifiedTime: info.ModTime(),
	}

	if !info.IsDir() {
		ext := filepath.Ext(info.Name())
		if mimeType := mime.TypeByExtension(ext); mimeType != "" {
			fi.MimeType = mimeType
		}

		if data, err := os.ReadFile(absPath); err == nil {
			hash := sha256.Sum256(data)
			fi.ContentHash = fmt.Sprintf("%x", hash)
		}
	}

	return fi
}
