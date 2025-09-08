package main

import (
	"context"
	"fmt"
	"log"
	"mythdb/pkg/lsm"
	"time"
)

func main() {
	fmt.Println("MythDB v3 Demo - LSM Tree Database")
	fmt.Println("==================================")

	// Create configuration
	config := &lsm.Config{
		DataDir:             "./demo-data",
		MaxMemTableSize:     100,
		MaxLevels:           3,
		LevelSizeMultiplier: 5,
		CompactionThreshold: 2,
	}

	// Initialize LSM tree
	db, err := lsm.NewLSM(config)
	if err != nil {
		log.Fatalf("Failed to initialize LSM tree: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	// Demo 1: Basic operations
	fmt.Println("\n1. Basic Operations:")
	fmt.Println("-------------------")

	// Put some data
	keys := []string{"user:1", "user:2", "user:3", "product:1", "product:2"}
	values := []string{"John Doe", "Jane Smith", "Bob Johnson", "Laptop", "Mouse"}

	for i, key := range keys {
		start := time.Now()
		err := db.Put(ctx, []byte(key), []byte(values[i]))
		duration := time.Since(start)

		if err != nil {
			fmt.Printf("❌ Failed to put %s: %v\n", key, err)
		} else {
			fmt.Printf("✅ Put %s = %s (took %v)\n", key, values[i], duration)
		}
	}

	// Get data
	fmt.Println("\nRetrieving data:")
	for _, key := range keys {
		start := time.Now()
		entry, err := db.Get(ctx, []byte(key))
		duration := time.Since(start)

		if err != nil {
			fmt.Printf("❌ Failed to get %s: %v\n", key, err)
		} else if entry == nil {
			fmt.Printf("❌ Key %s not found\n", key)
		} else if entry.Tombstone {
			fmt.Printf("❌ Key %s was deleted\n", key)
		} else {
			fmt.Printf("✅ Get %s = %s (took %v)\n", key, string(entry.Value), duration)
		}
	}

	// Demo 2: Update operations
	fmt.Println("\n2. Update Operations:")
	fmt.Println("--------------------")

	// Update existing key
	start := time.Now()
	err = db.Put(ctx, []byte("user:1"), []byte("John Updated"))
	duration := time.Since(start)

	if err != nil {
		fmt.Printf("❌ Failed to update user:1: %v\n", err)
	} else {
		fmt.Printf("✅ Updated user:1 (took %v)\n", duration)
	}

	// Verify update
	entry, err := db.Get(ctx, []byte("user:1"))
	if err != nil {
		fmt.Printf("❌ Failed to get updated user:1: %v\n", err)
	} else {
		fmt.Printf("✅ Updated value: %s\n", string(entry.Value))
	}

	// Demo 3: Delete operations
	fmt.Println("\n3. Delete Operations:")
	fmt.Println("--------------------")

	// Delete a key
	start = time.Now()
	err = db.Delete(ctx, []byte("product:2"))
	duration = time.Since(start)

	if err != nil {
		fmt.Printf("❌ Failed to delete product:2: %v\n", err)
	} else {
		fmt.Printf("✅ Deleted product:2 (took %v)\n", duration)
	}

	// Try to get deleted key
	entry, err = db.Get(ctx, []byte("product:2"))
	if err != nil {
		fmt.Printf("❌ Failed to get deleted product:2: %v\n", err)
	} else if entry == nil {
		fmt.Printf("✅ product:2 correctly deleted (not found)\n")
	} else {
		fmt.Printf("❌ product:2 should be deleted but found: %s\n", string(entry.Value))
	}

	// Demo 4: Performance test
	fmt.Println("\n4. Performance Test:")
	fmt.Println("-------------------")

	numOperations := 1000
	fmt.Printf("Performing %d put operations...\n", numOperations)

	start = time.Now()
	for i := 0; i < numOperations; i++ {
		key := fmt.Sprintf("perf:key:%d", i)
		value := fmt.Sprintf("perf:value:%d", i)
		if err := db.Put(ctx, []byte(key), []byte(value)); err != nil {
			fmt.Printf("❌ Failed to put %s: %v\n", key, err)
			break
		}
	}
	duration = time.Since(start)

	fmt.Printf("✅ Completed %d puts in %v (avg: %v per operation)\n",
		numOperations, duration, duration/time.Duration(numOperations))

	// Test reads
	fmt.Printf("Performing %d get operations...\n", numOperations/2)

	start = time.Now()
	for i := 0; i < numOperations/2; i++ {
		key := fmt.Sprintf("perf:key:%d", i)
		entry, err := db.Get(ctx, []byte(key))
		if err != nil {
			fmt.Printf("❌ Failed to get %s: %v\n", key, err)
			break
		}
		if entry == nil {
			fmt.Printf("❌ Key %s not found\n", key)
			break
		}
	}
	duration = time.Since(start)

	fmt.Printf("✅ Completed %d gets in %v (avg: %v per operation)\n",
		numOperations/2, duration, duration/time.Duration(numOperations/2))

	// Demo 5: MemTable flush simulation
	fmt.Println("\n5. MemTable Flush Simulation:")
	fmt.Println("-----------------------------")

	// Fill memtable to trigger flush
	fmt.Println("Filling memtable to trigger flush...")

	for i := 0; i < 150; i++ { // More than MaxMemTableSize (100)
		key := fmt.Sprintf("flush:key:%d", i)
		value := fmt.Sprintf("flush:value:%d", i)
		if err := db.Put(ctx, []byte(key), []byte(value)); err != nil {
			fmt.Printf("❌ Failed to put %s: %v\n", key, err)
			break
		}
	}

	fmt.Println("✅ Memtable should have been flushed to SSTable")

	// Verify data is still accessible after flush
	entry, err = db.Get(ctx, []byte("flush:key:50"))
	if err != nil {
		fmt.Printf("❌ Failed to get flushed data: %v\n", err)
	} else if entry == nil {
		fmt.Printf("❌ Flushed data not found\n")
	} else {
		fmt.Printf("✅ Flushed data accessible: %s\n", string(entry.Value))
	}

	fmt.Println("\n🎉 Demo completed successfully!")
	fmt.Println("\nTo explore more:")
	fmt.Println("- Run HTTP server: go run cmd/server/main.go")
	fmt.Println("- Use CLI tool: go run cmd/cli/main.go ./demo-data")
	fmt.Println("- Check data directory: ls -la ./demo-data")
}
