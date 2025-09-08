# LSM Tree Implementation in Go

Đây là một implementation hoàn chỉnh của cây LSM (Log-Structured Merge Tree) bằng Golang với các thành phần chính:

## Các thành phần chính

### 1. KeyValue (`pkg/lsm/types.go`)
- Đại diện cho một cặp key-value với timestamp và flag deleted
- Hỗ trợ encoding/decoding để lưu trữ
- So sánh key và timestamp

### 2. SkipList (`pkg/lsm/skiplist.go`)
- Implement skiplist cho MemTable
- Hỗ trợ insert, get, delete với độ phức tạp O(log n) trung bình
- Tự động tạo level ngẫu nhiên cho các node mới

### 3. WAL (Write-Ahead Log) (`pkg/lsm/wal.go`)
- Ghi log tất cả thao tác trước khi thực hiện
- Đảm bảo durability khi crash
- Hỗ trợ replay để khôi phục trạng thái

### 4. MemTable (`pkg/lsm/memtable.go`)
- Bảng trong bộ nhớ sử dụng skiplist
- Tích hợp với WAL để đảm bảo ACID
- Tự động flush khi đầy

### 5. SSTable (`pkg/lsm/sstable.go`)
- Bảng đã sắp xếp trên disk
- Có index và bloom filter để tối ưu truy vấn
- Hỗ trợ range query

### 6. Bloom Filter (`pkg/sstable/bloom_filter.go`)
- Sử dụng thư viện `github.com/bits-and-blooms/bloom/v3`
- Giảm false positive khi truy vấn
- Tự động tối ưu kích thước và số hash functions

### 7. Leveled Compaction (`pkg/lsm_tree/compaction.go`)
- Chiến lược compaction theo level
- Level 0: không giới hạn kích thước, giới hạn số bảng
- Level 1+: giới hạn kích thước và số bảng
- Tự động trigger compaction khi level đầy

### 8. LSM Tree (`pkg/lsm_tree/lsm_tree.go`)
- Kết hợp tất cả thành phần
- Quản lý MemTable và các level
- Hỗ trợ CRUD operations và range query

## Cách sử dụng

### 1. Tạo LSM Tree
```go
lsmTree, err := lsm.NewLSMTree("./data")
if err != nil {
    log.Fatal(err)
}
defer lsmTree.Close()
```

### 2. Thêm dữ liệu
```go
err := lsmTree.Put([]byte("key"), []byte("value"))
if err != nil {
    log.Printf("Failed to put: %v", err)
}
```

### 3. Lấy dữ liệu
```go
kv := lsmTree.Get([]byte("key"))
if kv != nil {
    if kv.Deleted {
        fmt.Println("Key is deleted")
    } else {
        fmt.Printf("Value: %s\n", string(kv.Value))
    }
}
```

### 4. Xóa dữ liệu
```go
err := lsmTree.Delete([]byte("key"))
if err != nil {
    log.Printf("Failed to delete: %v", err)
}
```

### 5. Range Query
```go
kvs := lsmTree.GetRange([]byte("a"), []byte("z"))
for _, kv := range kvs {
    fmt.Printf("%s: %s\n", string(kv.Key), string(kv.Value))
}
```

### 6. Thống kê
```go
stats := lsmTree.Stats()
fmt.Printf("MemTable size: %d\n", stats["memtable_size"])
```

## Chạy demo

```bash
# Tạo thư mục data
mkdir -p data

# Chạy demo
go run cmd/main.go
```

## Cấu trúc thư mục

```
mythdb/
├── pkg/
│   ├── types.go              # KeyValue struct (shared types)
│   ├── skiplist/
│   │   └── skiplist.go       # SkipList implementation
│   ├── wal/
│   │   └── wal.go            # Write-Ahead Log
│   ├── sstable/
│   │   ├── bloom_filter.go   # Bloom Filter using bits-and-blooms library
│   │   └── sstable.go        # SSTable implementation
│   ├── memtable/
│   │   └── memtable.go       # MemTable with skiplist
│   └── lsm_tree/
│       ├── compaction.go     # Leveled compaction strategy
│       └── lsm_tree.go       # Main LSM tree
├── cmd/
│   └── main.go               # Demo application
├── go.mod
└── README.md
```

## Đặc điểm kỹ thuật

- **MemTable**: Sử dụng skiplist với O(log n) trung bình
- **WAL**: Đảm bảo durability và crash recovery
- **SSTable**: Có index và bloom filter từ thư viện bits-and-blooms
- **Compaction**: Leveled strategy với 10 levels
- **Concurrency**: Thread-safe với RWMutex
- **Persistent**: Lưu trữ trên disk với cấu trúc tối ưu
- **Dependencies**: Sử dụng `github.com/bits-and-blooms/bloom/v3` cho bloom filter

## Hiệu suất

- **Write**: O(log n) cho MemTable, O(1) cho WAL
- **Read**: O(log n) cho MemTable, O(log n) cho SSTable
- **Compaction**: Background process, không block operations
- **Memory**: Giới hạn bởi MemTable size

## Mở rộng

Có thể mở rộng thêm:
- Compression cho SSTable
- Multiple MemTables
- Background compaction workers
- Metrics và monitoring
- Configuration management
