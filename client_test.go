package fdfs_client

import (
	"fmt"
	"sync"
	"testing"
)

var client *Client

func TestMain(m *testing.M) {
	fmt.Println("start")
	var err error
	client, err = NewClient(WithMaxConns(10), WithTrackerAddr([]string{
		"192.168.1.223:22122",
	}))
	defer client.Destory()
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	m.Run()
	defer client.Destory()
	fmt.Println("end")
}

func TestUpload(t *testing.T) {
	fileId, err := client.UploadByFilename("client_test.go")
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	fmt.Println(fileId)
	if err := client.DownloadToFile(fileId, "tempFile", 0, 0); err != nil {
		fmt.Println(err.Error())
		return
	}
	if buffer, err := client.DownloadToBuffer(fileId, 0, 19); err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println(string(buffer))
	}
	if err := client.DeleteFile(fileId); err != nil {
		fmt.Println(err.Error())
		return
	}
}

func TestUploadFile100(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i != 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j != 10; j++ {
				if fileId, err := client.UploadByFilename("client_test.go"); err != nil {
					fmt.Println(err.Error())
				} else {
					//fmt.Println(fileId)

					if _, err := client.DownloadToBuffer(fileId, 0, 19); err != nil {
						fmt.Println(err.Error())
					}
					if err := client.DeleteFile(fileId); err != nil {
						fmt.Println(err.Error())
					}
				}
			}
		}()
	}
	wg.Wait()
}

func TestUploadBuffer100(t *testing.T) {
	var wg sync.WaitGroup
	for i := 0; i != 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j != 10; j++ {
				if fileId, err := client.UploadByBuffer([]byte("hello world"), "go"); err != nil {
					fmt.Println(err.Error())
				} else {
					//fmt.Println(fileId)

					if _, err := client.DownloadToBuffer(fileId, 0, 11); err != nil {
						fmt.Println(err.Error())
					}
					if err := client.DeleteFile(fileId); err != nil {
						fmt.Println(err.Error())
					}
				}
			}
		}()
	}
	wg.Wait()
}
