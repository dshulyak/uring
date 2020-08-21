Golang library for io_uring framework (without CGO)
===

- (io_uring intro)[https://kernel.dk/io_uring.pdf]
- https://github.com/axboe/liburing

## Benchmarks

DO 8 - 32Gb - 100 SSD

#### OS-128

BenchmarkReadAt
BenchmarkReadAt/os_4096
BenchmarkReadAt/os_4096-128                10000             24440 ns/op         167.60 MB/s          29 B/op          0 allocs/op
BenchmarkReadAt/os_8192
BenchmarkReadAt/os_8192-128                10000              9506 ns/op         861.79 MB/s           1 B/op          0 allocs/op
BenchmarkReadAt/os_16384
BenchmarkReadAt/os_16384-128               10000             11809 ns/op	1387.37 MB/s           2 B/op          0 allocs/op
BenchmarkReadAt/os_65536
BenchmarkReadAt/os_65536-128               10000             28023 ns/op	2338.64 MB/s           3 B/op          0 allocs/op
BenchmarkReadAt/os_262144
BenchmarkReadAt/os_262144-128              10000             93838 ns/op	2793.57 MB/s           3 B/op          0 allocs/op
BenchmarkReadAt/os_1048576
BenchmarkReadAt/os_1048576-128             10000            360907 ns/op	2905.39 MB/s           0 B/op          0 allocs/op

#### Sharded eventfd

BenchmarkReadAt
BenchmarkReadAt/uring_sharded_default_4096
BenchmarkReadAt/uring_sharded_default_4096-8               10000             28864 ns/op         141.91 MB/s         821 B/op          4 allocs/op
BenchmarkReadAt/uring_sharded_default_8192
BenchmarkReadAt/uring_sharded_default_8192-8               10000              9799 ns/op         836.02 MB/s         192 B/op          3 allocs/op
BenchmarkReadAt/uring_sharded_default_16384
BenchmarkReadAt/uring_sharded_default_16384-8              10000             11277 ns/op	1452.84 MB/s         225 B/op          4 allocs/op
BenchmarkReadAt/uring_sharded_default_65536
BenchmarkReadAt/uring_sharded_default_65536-8              10000             27234 ns/op	2406.37 MB/s         168 B/op          3 allocs/op
BenchmarkReadAt/uring_sharded_default_262144
BenchmarkReadAt/uring_sharded_default_262144-8             10000             93186 ns/op	2813.12 MB/s         260 B/op          4 allocs/op
BenchmarkReadAt/uring_sharded_default_1048576
BenchmarkReadAt/uring_sharded_default_1048576-8            10000            364822 ns/op	2874.22 MB/s         721 B/op          4 allocs/op
PASS

#### Sharded enter

BenchmarkReadAt
BenchmarkReadAt/uring_sharded_enter_4096
BenchmarkReadAt/uring_sharded_enter_4096-8                 10000             26914 ns/op         152.19 MB/s         613 B/op          4 allocs/op
BenchmarkReadAt/uring_sharded_enter_8192
BenchmarkReadAt/uring_sharded_enter_8192-8                 10000              9236 ns/op         886.92 MB/s         246 B/op          4 allocs/op
BenchmarkReadAt/uring_sharded_enter_16384
BenchmarkReadAt/uring_sharded_enter_16384-8                10000             10387 ns/op	1577.35 MB/s         186 B/op          3 allocs/op
BenchmarkReadAt/uring_sharded_enter_65536
BenchmarkReadAt/uring_sharded_enter_65536-8                10000             27047 ns/op	2423.07 MB/s         446 B/op          4 allocs/op
BenchmarkReadAt/uring_sharded_enter_262144
BenchmarkReadAt/uring_sharded_enter_262144-8               10000             93096 ns/op	2815.85 MB/s         231 B/op          3 allocs/op
BenchmarkReadAt/uring_sharded_enter_1048576
BenchmarkReadAt/uring_sharded_enter_1048576-8              10000            370215 ns/op	2832.34 MB/s         145 B/op          3 allocs/op
PASS