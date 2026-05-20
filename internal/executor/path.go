package executor

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// ValidatePath ensures a path is within one of the allowed directories.
func ValidatePath(path string, allowedDirs []string) (string, error) {
	cleaned := filepath.Clean(path)

	absPath, err := filepath.Abs(cleaned)
	if err != nil {
		return "", fmt.Errorf("resolving absolute path: %w", err)
	}

	resolved, err := filepath.EvalSymlinks(absPath)
	if err != nil {
		return "", fmt.Errorf("resolving symlinks: %w", err)
	}

	blocked := []string{"/proc", "/sys", "/dev", "/run", "/tmp/beluga-"}
	for _, prefix := range blocked {
		if strings.HasPrefix(resolved, prefix) {
			return "", fmt.Errorf("path %q is in a blocked directory", path)
		}
	}

	for _, dir := range allowedDirs {
		allowedAbs, err := filepath.Abs(filepath.Clean(dir))
		if err != nil {
			continue
		}
		allowedResolved, err := filepath.EvalSymlinks(allowedAbs)
		if err != nil {
			allowedResolved = allowedAbs
		}
		if resolved == allowedResolved || strings.HasPrefix(resolved, allowedResolved+string(os.PathSeparator)) {
			return resolved, nil
		}
	}

	return "", fmt.Errorf("path %q is outside allowed directories", path)
}

// ValidateWorkingDir validates a working directory.
// If workingDir is empty, it returns the first allowed directory.
func ValidateWorkingDir(workingDir string, allowedDirs []string) (string, error) {
	if workingDir == "" {
		if len(allowedDirs) == 0 {
			return "", fmt.Errorf("no allowed directories configured")
		}
		return allowedDirs[0], nil
	}
	return ValidatePath(workingDir, allowedDirs)
}
