Golang library for io_uring framework (without CGO)
===

io_uring is the new kernel interface for asynchronous IO. The best introduction is
[io_uring intro](https://kernel.dk/io_uring.pdf).

Benchmarks
===

Benchmarks are collected on 5.8.3 kernel, ext4 and old ssd drive. File is opened with O_DIRECT.

BenchmarkReadAt/uring_512-8              8000000              1879 ns/op         272.55 MB/s
BenchmarkReadAt/uring_8192-8             1000000             18178 ns/op         450.65 MB/s

BenchmarkReadAt/os_512-256               8000000              4393 ns/op         116.55 MB/s
BenchmarkReadAt/os_8192-256              1000000             18811 ns/op         435.48 MB/s

Implementation
===

#### memory ordering (atomics)

liburing is using atomics to guarantee that write to submission queue will be visible by the kernel when sq tail is updated, and vice versa with head for completions.

Golang atomics provide stronger guarantees ([#1](https://github.com/golang/go/issues/32428),[#2](https://github.com/golang/go/issues/35639)) than what is necessary, however this guarantees are not explicitly specified and may change unexpectadly. It is very unlikely that it will happen in a way that will break this library, but it may.

Also, runtime/internal/atomic module has implementation for weaker atomics that provide exactly memory_order_release and memory_order_acquire. You can link them using `go:linkname` pragma, but it is not safe, nobody wants about it and it doesn't provide any noticeable perf improvements to justify this hack.

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

In this example sqe.addr may become invalid right after Writev helper returns. In order to lock pointer in place there is hidden pragma `go:uintptrescapes`.

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

On linux we can use syscall.Gettid efficiently to assign work to a particular ring in a way that minimizes contetion. Completions can be reapeted by registering eventfd for every ring, and watching all of them with single epoll instance. It is critical to ensure that completion path doesn't use any synchronization as the performance plumets if it does.

In runtime we can use gopark/goready directly, however this is not available outside of the runtime and I had to use simple channel for notifying submitter on completion. This works nicely and doesn't introduce a lot of overhead. This whole approach in general adds ~750ns with high submition rate (includes spawning goroutine, submitting nop uring operation, and waiting for completion).

Several weak points of this approach are:

- IOPOLL and SQPOLL can't be used, as it will lead to creation of a polling thread for each CPU.
- Submissions are not batched (syscall per operation).
  However this can be improved with a batch API.

By introducing uring into the runtime directly we can decrease overhead even futher, by removing syscall.Gettid, removing sq synchronization, and improving gopark/goready efficiency (if compared with waiting on channel).