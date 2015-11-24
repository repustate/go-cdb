package portablemmap

import (
	"fmt"
	"os"
)

func Prefault(mmapedData []byte) {
	// This is a no-op on Windows.
}

func Mmap(f *os.File) ([]byte, error) {
	return nil, fmt.Errorf("IMPLEMENT ME!")
}

func Munmap(mmappedData []byte) error {
	return fmt.Errorf("IMPLEMENT ME!")
}
