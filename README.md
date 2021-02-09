Golang library for io_uring framework (without CGO)
===

io_uring is the new kernel interface for asynchronous IO. The best introduction is
[io_uring intro](https://kernel.dk/io_uring.pdf).

Note that this library is mostly tested on 5.8.* kernel. While the core of this library
doesn't use any of the newer features and will work on the kernel with io_uring that
supports flags IORING_SETUP_CQSIZE and IORING_SETUP_ATTACH_WQ, and supports notifications with eventfd (IORING_REGISTER_EVENTFD) - some of the tests will depend on the latest features and will probably fail with cryptic errors if run on the kernel that doesn't support those features.

Benchmarks
===

Benchmarks for reading 40gb file are collected on 5.8.15 kernel, ext4 and Samsung EVO 960. File is opened with O_DIRECT. Benchmarks are comparing the fastest way to read a file using optimal strategies with io_uring or os.

#### io_uring

- 16 rings (one per core) with shared kernel workers
- one thread per ring to reap completions (faster than single thread with epoll and eventfd's)
- 4096 submission queue size
- 8192 completion queue size
- 100 000 concurrent readers

```
BenchmarkReadAt/enter_4096-16            5000000          1709 ns/op	2397.39 MB/s          34 B/op          2 allocs/op

```

#### os

- 128 os threads (more than that hurts performance)

```
BenchmarkReadAt/os_4096-128              5000000          1901 ns/op	2154.84 MB/s           0 B/op          0 allocs/op
```

Implementation
===

#### memory ordering (atomics)

io_uring relies on StoreRelease/ReadAcquire atomic semantics to guarantee that submitted entries will be visible after by the kernel when sq tail is updated, and vice verse for completed and cq head.

Based on comments ([#1](https://github.com/golang/go/issues/32428),[#2](https://github.com/golang/go/issues/35639)) golang provides sufficient guarantees (actually stronger) for this to work. However i wasn't able to find any mention in memory model specification so it is a subject to change, but highly unlikely.

Also, runtime/internal/atomic module has implementation for weaker atomics that provide exactly StoreRelease and ReadAcquire. You can link them using `go:linkname` pragma, but it is not safe, nobody wants about it and it doesn't provide any noticeable perf improvements to justify this hack.

```go
//go:linkname LoadAcq runtime/internal/atomic.LoadAcq
func LoadAcq(ptr *uint32) uint32

//go:linkname StoreRel runtime/internal/atomic.StoreRel
func StoreRel(ptr *uint32, val uint32)
```

#### pointers

Casting pointers to uintptr is generally unsafe as there is no guarantee that they will remain in the same location after the cast. Documentation for unsafe.Pointer specifies in what situations it can be done safely. Communication with io_uring, obviously, assumes that pointer will be valid until the end of the execution. Take a look at the writev syscall example:

```go
func Writev(sqe *SQEntry, fd uintptr, iovec []syscall.Iovec, offset uint64, flags uint32) {
        sqe.opcode = IORING_OP_WRITEV
        sqe.fd = int32(fd)
        sqe.len = uint32(len(iovec))
        sqe.offset = offset
        sqe.opcodeFlags = flags
        sqe.addr = (uint64)(uintptr(unsafe.Pointer(&iovec[0])))
}
```

In this example `sqe.addr` may become invalid right after Writev helper returns. In order to lock pointer in place there is hidden pragma `go:uintptrescapes`.

```go
//go:uintptrescapes

func (q *Queue) Syscall(opts func(*uring.SQEntry), ptrs ...uintptr) (uring.CQEntry, error) {
        return q.Complete(opts)
}

...

func (f *File) WriteAt(buf []byte, off int64) (int, error) {
        if len(buf) == 0 {
                return 0, nil
        }
        iovec := []syscall.Iovec{{Base: &buf[0], Len: uint64(len(buf))}}
        return ioRst(f.queue.Syscall(func(sqe *uring.SQEntry) {
                uring.Writev(sqe, f.ufd, iovec, uint64(off), 0)
                sqe.SetFlags(f.flags)
        }, uintptr(unsafe.Pointer(&iovec[0]))))
}
```

`ptrs` are preventing pointers from being moved to another location until the Syscall exits.

This approach has several limitations:

- pragma `go:uintptrescapes` forces heap allocation (e.g. iovec will escape to the heap in this example).
- you cannot use interface for queue.Syscall.

It must be possible to achieve the same without heap allocation (e.g. the same way as in syscall.Syscall/syscall.RawSyscall).

#### synchronization and goroutines

Submissions queue requires synchronization if used concurrently by multiple goroutines. It leads to contention with large number of CPU's. The natural way to avoid contetion is to setup ring per thread, io_uring provides handy flag IORING_SETUP_ATTACH_WQ that allows to share same kernel pool between multiple rings.

On linux we can use syscall.Gettid efficiently to assign work to a particular ring in a way that minimizes contetion. It is also critical to ensure that completion path doesn't have to synchronize with submission, as it introductes noticeable degradation.

Another potential unsafe improvement is to link procPin from runtime. And use it in the place of syscall.Gettid, my last benchmark shows no difference (maybe couple ns not in favor of procPing/procUnpin).

```go
//go:linkname procPin runtime.procPin
func procPin() int

//go:linkname procUnpin runtime.procUnpin
func procUnpin() int
```

In runtime we can use gopark/goready directly, however this is not available outside of the runtime and I had to use simple channel for notifying submitter on completion. This works nicely and doesn't introduce a lot of overhead. This whole approach in general adds ~750ns with high submission rate (includes spawning goroutine, submitting nop uring operation, and waiting for completion).

Several weak points of this approach are:

- IOPOLL and SQPOLL can't be used, as it will lead to creation of a polling thread for each CPU.
- Submissions are not batched (syscall per operation).
  However this can be improved with a batch API.

By introducing uring into the runtime directly we can decrease overhead even futher, by removing syscall.Gettid, removing sq synchronization, and improving gopark/goready efficiency (if compared with waiting on channel).
