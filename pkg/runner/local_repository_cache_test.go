package runner

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLocalRepositoryCacheConcurrentAccess(t *testing.T) {
	actionPath := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(actionPath, "action.yml"), []byte("name: test\n"), 0o644))

	cache := &LocalRepositoryCache{
		LocalRepositories: map[string]string{
			"example/action@v1": actionPath,
		},
		CacheDirCache: make(map[string]string),
	}
	const workers = 32
	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ref, err := cache.Fetch(context.Background(), "example/action", "https://github.com/example/action", "v1", "")
			if err != nil {
				errs <- err
				return
			}
			archive, err := cache.GetTarArchive(context.Background(), "example/action", ref, ".")
			if err != nil {
				errs <- err
				return
			}
			defer archive.Close()
			if _, err := io.Copy(io.Discard, archive); err != nil {
				errs <- fmt.Errorf("read local action archive: %w", err)
			}
		}()
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}
