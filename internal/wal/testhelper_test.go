package wal

import "os"

// 测试里用到的薄封装，避免测试文件直接依赖 os 细节。
func osReadFile(path string) ([]byte, error)  { return os.ReadFile(path) }
func osWriteFile(path string, b []byte) error { return os.WriteFile(path, b, 0o644) }
