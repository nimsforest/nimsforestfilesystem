package nimsforestfilesystem

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/nimsforest/nimsforest2/pkg/bedrock"
	"github.com/nimsforest/nimsforest2/pkg/nim"
)

// UnixBedrock is a persistent storage implementation backed by a plain filesystem.
// No git, no audit trail — fastest writes. Suitable for scratch space, caches,
// and high-frequency write scenarios.
type UnixBedrock struct {
	name     string
	path     string
	readOnly bool
	wind     *nim.Wind

	mu sync.RWMutex
}

// NewUnixBedrock creates a new unix filesystem bedrock instance.
func NewUnixBedrock(name, path string, wind *nim.Wind) *UnixBedrock {
	return &UnixBedrock{
		name: name,
		path: path,
		wind: wind,
	}
}

// Start ensures the directory exists and emits a mounted event.
func (u *UnixBedrock) Start(ctx context.Context) error {
	if err := os.MkdirAll(u.path, 0755); err != nil {
		return fmt.Errorf("creating bedrock directory: %w", err)
	}

	log.Printf("[Bedrock:%s] Started (type=unix, path=%s)", u.name, u.path)

	if u.wind != nil {
		event := bedrock.MountedEvent{
			BedrockName: u.name,
			BedrockType: "unix",
			ReadOnly:    u.readOnly,
		}
		data, _ := json.Marshal(event)
		leaf := nim.NewLeaf(bedrock.Subject(u.name, bedrock.EventMounted), data, "bedrock."+u.name)
		if err := u.wind.Drop(*leaf); err != nil {
			log.Printf("[Bedrock:%s] Failed to emit mounted event: %v", u.name, err)
		}
	}

	return nil
}

// Stop gracefully shuts down the bedrock.
func (u *UnixBedrock) Stop() error {
	log.Printf("[Bedrock:%s] Stopped", u.name)
	return nil
}

// Name returns the configured name.
func (u *UnixBedrock) Name() string { return u.name }

// Type returns "unix".
func (u *UnixBedrock) Type() string { return "unix" }

// IsReadOnly returns whether this bedrock supports writes.
func (u *UnixBedrock) IsReadOnly() bool { return u.readOnly }

// List returns file information for entries in the given directory path.
func (u *UnixBedrock) List(path string) ([]bedrock.FileInfo, error) {
	u.mu.RLock()
	defer u.mu.RUnlock()

	absPath := u.resolve(path)
	if !u.isWithinRoot(absPath) {
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
func (u *UnixBedrock) Read(path string) ([]byte, error) {
	u.mu.RLock()
	defer u.mu.RUnlock()

	absPath := u.resolve(path)
	if !u.isWithinRoot(absPath) {
		return nil, fmt.Errorf("path traversal not allowed: %s", path)
	}

	return os.ReadFile(absPath)
}

// Stat returns metadata about a single file or directory.
func (u *UnixBedrock) Stat(path string) (*bedrock.FileInfo, error) {
	u.mu.RLock()
	defer u.mu.RUnlock()

	absPath := u.resolve(path)
	if !u.isWithinRoot(absPath) {
		return nil, fmt.Errorf("path traversal not allowed: %s", path)
	}

	info, err := os.Stat(absPath)
	if err != nil {
		return nil, err
	}

	fi := fileInfoFromOS(path, absPath, info)
	return &fi, nil
}

// Write creates or updates a file.
func (u *UnixBedrock) Write(path string, content []byte, options ...bedrock.WriteOption) error {
	if u.readOnly {
		return bedrock.ErrReadOnly{BedrockName: u.name}
	}

	u.mu.Lock()
	defer u.mu.Unlock()

	absPath := u.resolve(path)
	if !u.isWithinRoot(absPath) {
		return fmt.Errorf("path traversal not allowed: %s", path)
	}

	_, err := os.Stat(absPath)
	isCreate := os.IsNotExist(err)

	if err := os.MkdirAll(filepath.Dir(absPath), 0755); err != nil {
		return fmt.Errorf("creating parent directory: %w", err)
	}

	if err := os.WriteFile(absPath, content, 0644); err != nil {
		return fmt.Errorf("writing file: %w", err)
	}

	eventType := bedrock.EventFileModified
	if isCreate {
		eventType = bedrock.EventFileCreated
	}
	u.emitFileEvent(eventType, path)

	return nil
}

// Delete removes a file.
func (u *UnixBedrock) Delete(path string, options ...bedrock.WriteOption) error {
	if u.readOnly {
		return bedrock.ErrReadOnly{BedrockName: u.name}
	}

	u.mu.Lock()
	defer u.mu.Unlock()

	absPath := u.resolve(path)
	if !u.isWithinRoot(absPath) {
		return fmt.Errorf("path traversal not allowed: %s", path)
	}

	if _, err := os.Stat(absPath); os.IsNotExist(err) {
		return fmt.Errorf("file does not exist: %s", path)
	}

	if err := os.Remove(absPath); err != nil {
		return fmt.Errorf("removing file: %w", err)
	}

	u.emitFileEvent(bedrock.EventFileDeleted, path)

	return nil
}

func (u *UnixBedrock) resolve(path string) string {
	if path == "" || path == "/" {
		return u.path
	}
	return filepath.Join(u.path, filepath.FromSlash(path))
}

func (u *UnixBedrock) isWithinRoot(absPath string) bool {
	cleaned := filepath.Clean(absPath)
	return strings.HasPrefix(cleaned, u.path)
}

func (u *UnixBedrock) emitFileEvent(eventType, path string) {
	if u.wind == nil {
		return
	}

	absPath := u.resolve(path)
	info, err := os.Stat(absPath)

	var fi bedrock.FileInfo
	if err == nil {
		fi = fileInfoFromOS(path, absPath, info)
	} else {
		fi = bedrock.FileInfo{
			Path:     path,
			FileName: filepath.Base(path),
		}
	}

	event := bedrock.FileEvent{
		BedrockName: u.name,
		BedrockType: "unix",
		File:        fi,
	}
	data, _ := json.Marshal(event)
	subject := bedrock.Subject(u.name, eventType)
	leaf := nim.NewLeaf(subject, data, "bedrock."+u.name)
	if err := u.wind.Drop(*leaf); err != nil {
		log.Printf("[Bedrock:%s] Failed to emit %s event for %s: %v", u.name, eventType, path, err)
	}
}
