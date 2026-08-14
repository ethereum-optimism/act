package runner

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	git "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//nolint:gosec
func TestActionCache(t *testing.T) {
	a := assert.New(t)
	cache := &GoGitActionCache{
		Path: os.TempDir(),
	}
	ctx := context.Background()
	cacheDir := "nektos/act-test-actions"
	repo := "https://github.com/nektos/act-test-actions"
	refs := []struct {
		Name     string
		CacheDir string
		Repo     string
		Ref      string
	}{
		{
			Name:     "Fetch Branch Name",
			CacheDir: cacheDir,
			Repo:     repo,
			Ref:      "main",
		},
		{
			Name:     "Fetch Branch Name Absolutely",
			CacheDir: cacheDir,
			Repo:     repo,
			Ref:      "refs/heads/main",
		},
		{
			Name:     "Fetch HEAD",
			CacheDir: cacheDir,
			Repo:     repo,
			Ref:      "HEAD",
		},
		{
			Name:     "Fetch Sha",
			CacheDir: cacheDir,
			Repo:     repo,
			Ref:      "de984ca37e4df4cb9fd9256435a3b82c4a2662b1",
		},
	}
	for _, c := range refs {
		t.Run(c.Name, func(_ *testing.T) {
			sha, err := cache.Fetch(ctx, c.CacheDir, c.Repo, c.Ref, "")
			if !a.NoError(err) || !a.NotEmpty(sha) {
				return
			}
			atar, err := cache.GetTarArchive(ctx, c.CacheDir, sha, "js")
			if !a.NoError(err) || !a.NotEmpty(atar) {
				return
			}
			mytar := tar.NewReader(atar)
			th, err := mytar.Next()
			if !a.NoError(err) || !a.NotEqual(0, th.Size) {
				return
			}
			buf := &bytes.Buffer{}
			// G110: Potential DoS vulnerability via decompression bomb (gosec)
			_, err = io.Copy(buf, mytar)
			a.NoError(err)
			str := buf.String()
			a.NotEmpty(str)
		})
	}
}

func TestActionCacheConcurrentAccess(t *testing.T) {
	repoPath := t.TempDir()
	repo, err := git.PlainInit(repoPath, false)
	require.NoError(t, err)
	worktree, err := repo.Worktree()
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path.Join(repoPath, "action.yml"), []byte("name: test\n"), 0o644))
	_, err = worktree.Add("action.yml")
	require.NoError(t, err)
	_, err = worktree.Commit("initial", &git.CommitOptions{
		Author: &object.Signature{Name: "act", Email: "act@example.com", When: time.Now()},
	})
	require.NoError(t, err)

	cache := &GoGitActionCache{Path: t.TempDir()}
	const (
		cacheDir = "example/action"
		workers  = 8
	)
	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			sha, err := cache.Fetch(context.Background(), cacheDir, repoPath, "refs/heads/master", "")
			if err != nil {
				errs <- err
				return
			}
			archive, err := cache.GetTarArchive(context.Background(), cacheDir, sha, ".")
			if err != nil {
				errs <- err
				return
			}
			defer archive.Close()
			if _, err := io.Copy(io.Discard, archive); err != nil {
				errs <- fmt.Errorf("read action archive: %w", err)
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

func TestActionCacheOperationsAreSynchronized(t *testing.T) {
	repoPath := t.TempDir()
	repo, err := git.PlainInit(repoPath, false)
	require.NoError(t, err)
	worktree, err := repo.Worktree()
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path.Join(repoPath, "action.yml"), []byte("name: test\n"), 0o644))
	_, err = worktree.Add("action.yml")
	require.NoError(t, err)
	_, err = worktree.Commit("initial", &git.CommitOptions{
		Author: &object.Signature{Name: "act", Email: "act@example.com", When: time.Now()},
	})
	require.NoError(t, err)

	cache := &GoGitActionCache{Path: t.TempDir()}
	const cacheDir = "example/action"
	gitPath := path.Join(cache.Path, safeFilename(cacheDir)+".git")
	cacheLock := goGitActionCacheLock(gitPath)

	cacheLock.RLock()
	fetchDone := make(chan error, 1)
	go func() {
		_, err := cache.Fetch(context.Background(), cacheDir, repoPath, "refs/heads/master", "")
		fetchDone <- err
	}()
	select {
	case err := <-fetchDone:
		t.Fatalf("Fetch completed while the cache was read-locked: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	cacheLock.RUnlock()
	require.NoError(t, <-fetchDone)

	sha, err := cache.Fetch(context.Background(), cacheDir, repoPath, "refs/heads/master", "")
	require.NoError(t, err)
	archive, err := cache.GetTarArchive(context.Background(), cacheDir, sha, ".")
	require.NoError(t, err)

	writeLockAcquired := make(chan struct{})
	go func() {
		cacheLock.Lock()
		close(writeLockAcquired)
		cacheLock.Unlock()
	}()
	select {
	case <-writeLockAcquired:
		t.Fatal("cache write lock acquired before the archive reader was closed")
	case <-time.After(50 * time.Millisecond):
	}
	require.NoError(t, archive.Close())
	select {
	case <-writeLockAcquired:
	case <-time.After(5 * time.Second):
		t.Fatal("cache write lock remained blocked after the archive reader was closed")
	}
}

func TestActionCacheFailures(t *testing.T) {
	a := assert.New(t)
	cache := &GoGitActionCache{
		Path: os.TempDir(),
	}
	ctx := context.Background()
	cacheDir := "nektos/act-test-actions"
	repo := "https://github.com/nektos/act-test-actions-not-exist"
	repoExist := "https://github.com/nektos/act-test-actions"
	refs := []struct {
		Name     string
		CacheDir string
		Repo     string
		Ref      string
	}{
		{
			Name:     "Fetch Branch Name",
			CacheDir: cacheDir,
			Repo:     repo,
			Ref:      "main",
		},
		{
			Name:     "Fetch Branch Name Absolutely",
			CacheDir: cacheDir,
			Repo:     repo,
			Ref:      "refs/heads/main",
		},
		{
			Name:     "Fetch HEAD",
			CacheDir: cacheDir,
			Repo:     repo,
			Ref:      "HEAD",
		},
		{
			Name:     "Fetch Sha",
			CacheDir: cacheDir,
			Repo:     repo,
			Ref:      "de984ca37e4df4cb9fd9256435a3b82c4a2662b1",
		},
		{
			Name:     "Fetch Branch Name no existing",
			CacheDir: cacheDir,
			Repo:     repoExist,
			Ref:      "main2",
		},
		{
			Name:     "Fetch Branch Name Absolutely no existing",
			CacheDir: cacheDir,
			Repo:     repoExist,
			Ref:      "refs/heads/main2",
		},
		{
			Name:     "Fetch Sha no existing",
			CacheDir: cacheDir,
			Repo:     repoExist,
			Ref:      "de984ca37e4df4cb9fd9256435a3b82c4a2662b2",
		},
	}
	for _, c := range refs {
		t.Run(c.Name, func(t *testing.T) {
			_, err := cache.Fetch(ctx, c.CacheDir, c.Repo, c.Ref, "")
			t.Logf("%s\n", err)
			if !a.Error(err) {
				return
			}
		})
	}
}
