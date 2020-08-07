package uring

const (
	STATX_TYPE        uint32 = 1 << iota //Want stx_mode & S_IFMT
	STATX_MODE                           // Want stx_mode & ~S_IFMT
	STATX_NLINK                          // Want stx_nlink
	STATX_UID                            // Want stx_uid
	STATX_GID                            // Want stx_gid
	STATX_ATIME                          // Want stx_atime
	STATX_MTIME                          // Want stx_mtime
	STATX_CTIME                          // Want stx_ctime
	STATX_INO                            // Want stx_ino
	STATX_SIZE                           // Want stx_size
	STATX_BLOCKS                         // Want stx_blocks
	STATX_BASIC_STATS                    // [All of the above]
	STATX_BTIME                          // Want stx_btime
	STATX_ALL                            // [All currently available fields]
)

// StatxS is a structure used by STATX(2) syscall.
// man statx for field description.
type StatxS struct {
	Mask           uint32
	Blksize        uint32
	Attributes     uint64
	Nlink          uint32
	UID            uint32
	GID            uint32
	Mode           uint16
	Ino            uint64
	Size           uint64
	Block          uint64
	AttributesMask uint64

	Atime, Btime, Ctime, Mtime StatxTimestamp
	RdevMajor                  uint32
	RdevMinor                  uint32

	DevMajor uint32
	DevMinor uint32
}

// StatxTimestamp ...
type StatxTimestamp struct {
	Sec  int64
	Nsec uint32
}
