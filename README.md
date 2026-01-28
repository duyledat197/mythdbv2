# MythDB v2 - LSM Tree Implementation in Go

MythDB là một công cụ lưu trữ key-value (storage engine) hiệu năng cao, được xây dựng dựa trên cấu trúc **Log-Structured Merge Tree (LSM Tree)** bằng ngôn ngữ Go. Dự án tập trung vào việc hiện thực hóa các khái niệm cốt lõi của hệ quản trị cơ sở dữ liệu hiện đại.

## 🚀 Tính năng chính

- **MemTable (SkipList):** Lưu trữ tạm thời trên bộ nhớ sử dụng cấu trúc SkipList, tối ưu cho các thao tác đọc/ghi với độ phức tạp $O(\log n)$.
- **Write-Ahead Log (WAL):** Đảm bảo tính bền vững (durability) của dữ liệu. Khôi phục trạng thái hệ thống ngay lập tức sau khi crash.
- **SSTable (Sorted String Table):** Lưu trữ dữ liệu đã sắp xếp dưới dạng file trên đĩa cứng, tối ưu cho việc truy vấn dải (range query).
- **Persistent Index & Bloom Filter:** Giảm thiểu thao tác I/O bằng cách sử dụng chỉ mục trên đĩa và bộ lọc Bloom (từ thư viện `bits-and-blooms/bloom`).
- **Manifest Management:** Theo dõi và quản lý phiên bản của các SSTables trên các cấp độ khác nhau.
- **Leveled Compaction:** Chiến lược nén dữ liệu theo tầng giúp tối ưu bộ nhớ đĩa và cải thiện hiệu suất đọc.
- **Thread-Safety:** Hỗ trợ truy cập đa luồng an toàn bằng cơ chế `RWMutex`.

## 🛠 Cấu trúc thư mục

```text
mythdbv2/
├── cmd/
│   └── main.go           # Demo ứng dụng và ví dụ sử dụng
├── pkg/
│   ├── lsm/              # Logic cốt lõi của LSM Tree
│   ├── memtable/         # Hiện thực MemTable và Iterator
│   ├── sstable/          # Quản lý file SSTable, Index và Bloom Filter
│   ├── wal/              # Hiện thực Write-Ahead Log
│   ├── types/            # Các kiểu dữ liệu dùng chung (Entry, v.v.)
│   ├── manifest/         # Quản lý trạng thái các level và SSTables
│   └── priority_queue/   # Cấu trúc hàng đợi ưu tiên dùng cho Compaction
└── demo-data/            # Thư mục lưu trữ dữ liệu (tự động tạo)
```

## 📖 Hướng dẫn sử dụng

### 1. Khởi tạo Database

```go
import (
    "mythdb/pkg/lsm"
    "context"
)

config := lsm.DefaultConfig()
config.DataDir = "./my-data"

db, err := lsm.NewLSM(config)
if err != nil {
    panic(err)
}
defer db.Close()
```

### 2. Các thao tác cơ bản

```go
ctx := context.Background()

// Thêm hoặc cập nhật dữ liệu
err := db.Put(ctx, []byte("user:100"), []byte("Antigravity AI"))

// Truy vấn dữ liệu
entry, err := db.Get(ctx, []byte("user:100"))
if entry != nil {
    fmt.Printf("Value: %s\n", string(entry.Value))
}

// Xóa dữ liệu (sử dụng Tombstone)
err = db.Delete(ctx, []byte("user:100"))
```

### 3. Chạy Demo

Bạn có thể chạy thử nghiệm các tính năng thông qua file `main.go`:

```bash
go run cmd/main.go
```

## 📈 Thông số kỹ thuật

- **Ngôn ngữ:** Go (Golang) 1.25+
- **Kiến trúc:** LSM Tree với Leveled Compaction.
- **Dữ liệu:** Lưu trữ dưới dạng binary trên disk (`.sst`).
- **Nén:** Tự động nén khi số lượng file ở Level 0 vượt ngưỡng (mặc định là 2).

## 🤝 Đóng góp

Mọi đóng góp (Pull Request, Issue) đều được hoan nghênh để cải thiện MythDB. Cảm ơn các bạn!
