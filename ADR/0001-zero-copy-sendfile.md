Title: Zero-copy sendfile design

Status: Proposed

Context
-------
Phase 1 requires a zero-copy fetch path to maximize throughput. On Linux
`sendfile(2)` allows transferring file-backed page cache to a socket
without copying into user-space. macOS and other platforms do not provide
the same syscall semantics universally.

Decision
--------
Implement a platform-aware `SendFile` helper:
- Linux: use `syscall.Sendfile` to perform zero-copy when the destination
  is a socket-backed `*os.File`.
- Non-Linux: provide a safe fallback that streams chunks from the file to
  the writer.

Consequences
------------
- Performance: Linux will benefit from zero-copy; other platforms will
  use the fallback. Benchmarks must be run on Linux for Phase 1
  throughput goals.
- Correctness: fallback ensures behavior is identical across platforms.
