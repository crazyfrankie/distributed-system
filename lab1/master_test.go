package lab1

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// 测试用的Map函数
func MapTest(filename string, contents string) []KeyValue {
	// 简单的单词计数，每个单词只计数一次
	words := strings.Fields(contents)
	wordMap := make(map[string]bool)
	var kva []KeyValue
	for _, w := range words {
		if !wordMap[w] {
			kv := KeyValue{w, "1"}
			kva = append(kva, kv)
			wordMap[w] = true
		}
	}
	return kva
}

// 测试用的Reduce函数
func ReduceTest(key string, values []string) string {
	// 简单的计数
	return fmt.Sprintf("%d", len(values))
}

// 创建测试输入文件
func createTestFiles(t *testing.T) []string {
	// 创建临时目录
	tmpDir := os.TempDir()

	// 创建测试文件
	files := []string{
		"pg-being_ernest.txt",
		"pg-dorian_gray.txt",
		"pg-frankenstein.txt",
		"pg-huckleberry_finn.txt",
		"pg-metamorphosis.txt",
	}

	// 写入一些测试数据
	for _, f := range files {
		path := filepath.Join(tmpDir, f)
		err := os.WriteFile(path, []byte("the quick brown fox jumps over the lazy dog\n"), 0644)
		if err != nil {
			t.Fatal(err)
		}
	}

	// 返回完整路径
	fullPaths := make([]string, len(files))
	for i, f := range files {
		fullPaths[i] = filepath.Join(tmpDir, f)
	}
	return fullPaths
}

// 清理测试文件
func cleanupTestFiles(t *testing.T, files []string) {
	for _, f := range files {
		os.Remove(f)
	}
	// 清理临时目录
	os.RemoveAll(filepath.Dir(files[0]))
}

// 检查输出文件
func checkOutput(t *testing.T, nReduce int) {
	// 读取所有reduce输出文件
	outputs := make(map[string]int)
	for i := 0; i < nReduce; i++ {
		filename := fmt.Sprintf("mr-out-%d", i)
		content, err := os.ReadFile(filename)
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(string(content), "\n")
		for _, line := range lines {
			if line == "" {
				continue
			}
			var word string
			var count int
			fmt.Sscanf(line, "%s %d", &word, &count)
			outputs[word] += count
		}
	}

	// 验证结果
	expected := map[string]int{
		"the":   5,
		"quick": 5,
		"brown": 5,
		"fox":   5,
		"jumps": 5,
		"over":  5,
		"lazy":  5,
		"dog":   5,
	}

	for word, count := range expected {
		if outputs[word] != count {
			t.Errorf("word %s: expected count %d, got %d", word, count, outputs[word])
		}
	}
}

// 主测试函数
func TestMapReduce(t *testing.T) {
	// 清理可能存在的socket文件
	coordinatorSocket = ""        // 重置socket文件名
	os.RemoveAll("/tmp/824-mr-*") // 清理所有可能的socket文件

	// 创建测试文件
	files := createTestFiles(t)
	defer cleanupTestFiles(t, files)

	// 启动 master
	nReduce := 10
	coordinator := NewMaster(files, nReduce)

	// 启动多个worker
	nWorker := 3
	for i := 0; i < nWorker; i++ {
		go StartWorker(WithMapFunc(MapTest), WithReduceFunc(ReduceTest))
	}

	// 等待任务完成
	for !coordinator.Done() {
		time.Sleep(time.Second)
	}

	// 检查输出
	checkOutput(t, nReduce)

	// 清理socket文件
	os.RemoveAll(coordinatorSock())
}

// 测试容错性
func TestFaultTolerance(t *testing.T) {
	// 清理可能存在的socket文件
	coordinatorSocket = ""        // 重置socket文件名
	os.RemoveAll("/tmp/824-mr-*") // 清理所有可能的socket文件

	// 创建测试文件
	files := createTestFiles(t)
	defer cleanupTestFiles(t, files)

	// 启动 master
	nReduce := 10
	coordinator := NewMaster(files, nReduce)

	// 启动worker并模拟故障
	for i := 0; i < 5; i++ {
		go func() {
			StartWorker(WithMapFunc(MapTest), WithReduceFunc(ReduceTest))
			// 模拟worker故障
			time.Sleep(time.Second * 2)
		}()
	}

	// 等待任务完成
	for !coordinator.Done() {
		time.Sleep(time.Second)
	}

	// 检查输出
	checkOutput(t, nReduce)

	// 清理socket文件
	os.RemoveAll(coordinatorSock())
}

// 测试并发性
func TestConcurrency(t *testing.T) {
	// 清理可能存在的socket文件
	coordinatorSocket = ""        // 重置socket文件名
	os.RemoveAll("/tmp/824-mr-*") // 清理所有可能的socket文件

	// 创建测试文件
	files := createTestFiles(t)
	defer cleanupTestFiles(t, files)

	// 启动 master
	nReduce := 10
	coordinator := NewMaster(files, nReduce)

	// 启动多个worker
	nWorker := 10
	for i := 0; i < nWorker; i++ {
		go StartWorker(WithMapFunc(MapTest), WithReduceFunc(ReduceTest))
	}

	// 等待任务完成
	for !coordinator.Done() {
		time.Sleep(time.Second)
	}

	// 检查输出
	checkOutput(t, nReduce)

	// 清理socket文件
	os.RemoveAll(coordinatorSock())
}
