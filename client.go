package fdfs_client

import (
	"fmt"
	"net"
	"sync"
)

type Client struct {
	trackerPools    map[string]*connPool
	storagePools    map[string]*connPool
	storagePoolLock *sync.RWMutex
	config          *config
}

type ClientConfig func(*config)

func WithTrackerAddr(trackerAddr []string) ClientConfig {
	return func(c *config) {
		c.trackerAddr = trackerAddr
	}
}

func WithMaxConns(maxConns int) ClientConfig {
	return func(c *config) {
		c.maxConns = maxConns
	}
}

func NewClientWithConfig(configName string) (*Client, error) {
	config, err := newConfig(configName)
	if err != nil {
		return nil, err
	}
	client := &Client{
		config:          config,
		storagePoolLock: &sync.RWMutex{},
	}
	client.trackerPools = make(map[string]*connPool)
	client.storagePools = make(map[string]*connPool)

	for _, addr := range config.trackerAddr {
		trackerPool, err := newConnPool(addr, config.maxConns)
		if err != nil {
			return nil, err
		}
		client.trackerPools[addr] = trackerPool
	}

	return client, nil
}

func NewClient(options ...ClientConfig) (*Client, error) {
	dc := defaultConfig()
	for _, option := range options {
		option(dc)
	}

	client := &Client{
		config:          dc,
		storagePoolLock: &sync.RWMutex{},
	}
	client.trackerPools = make(map[string]*connPool)
	client.storagePools = make(map[string]*connPool)

	for _, addr := range dc.trackerAddr {
		trackerPool, err := newConnPool(addr, dc.maxConns)
		if err != nil {
			return nil, err
		}
		client.trackerPools[addr] = trackerPool
	}
	return client, nil
}

func (c *Client) Destory() {
	if c == nil {
		return
	}
	for _, pool := range c.trackerPools {
		pool.Destory()
	}
	for _, pool := range c.storagePools {
		pool.Destory()
	}
}

func (c *Client) UploadByFilename(fileName string) (string, error) {
	fileInfo, err := newFileInfo(fileName, nil, "")
	defer fileInfo.Close()
	if err != nil {
		return "", err
	}

	storageInfo, err := c.queryStorageInfoWithTracker(TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ONE, "", "")
	if err != nil {
		return "", err
	}

	task := &storageUploadTask{}
	//req
	task.fileInfo = fileInfo
	task.storagePathIndex = storageInfo.storagePathIndex

	if err := c.doStorage(task, storageInfo); err != nil {
		return "", err
	}
	return task.fileId, nil
}

func (c *Client) UploadByBuffer(buffer []byte, fileExtName string) (string, error) {
	fileInfo, err := newFileInfo("", buffer, fileExtName)
	defer fileInfo.Close()
	if err != nil {
		return "", err
	}
	storageInfo, err := c.queryStorageInfoWithTracker(TRACKER_PROTO_CMD_SERVICE_QUERY_STORE_WITHOUT_GROUP_ONE, "", "")
	if err != nil {
		return "", err
	}

	task := &storageUploadTask{}
	//req
	task.fileInfo = fileInfo
	task.storagePathIndex = storageInfo.storagePathIndex

	if err := c.doStorage(task, storageInfo); err != nil {
		return "", err
	}
	return task.fileId, nil
}

func (c *Client) DownloadToFile(fileId string, localFilename string, offset int64, downloadBytes int64) error {
	groupName, remoteFilename, err := splitFileId(fileId)
	if err != nil {
		return err
	}
	storageInfo, err := c.queryStorageInfoWithTracker(TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE, groupName, remoteFilename)
	if err != nil {
		return err
	}

	task := &storageDownloadTask{}
	//req
	task.groupName = groupName
	task.remoteFilename = remoteFilename
	task.offset = offset
	task.downloadBytes = downloadBytes

	//res
	task.localFilename = localFilename

	return c.doStorage(task, storageInfo)
}

// DownloadToBuffer deprecated
func (c *Client) DownloadToBuffer(fileId string, offset int64, downloadBytes int64) ([]byte, error) {
	groupName, remoteFilename, err := splitFileId(fileId)
	if err != nil {
		return nil, err
	}
	storageInfo, err := c.queryStorageInfoWithTracker(TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE, groupName, remoteFilename)
	if err != nil {
		return nil, err
	}

	task := &storageDownloadTask{}
	//req
	task.groupName = groupName
	task.remoteFilename = remoteFilename
	task.offset = offset
	task.downloadBytes = downloadBytes

	//res
	if err := c.doStorage(task, storageInfo); err != nil {
		return nil, err
	}
	return task.buffer, nil
}

func (c *Client) DownloadToAllocatedBuffer(fileId string, buffer []byte, offset int64, downloadBytes int64) error {
	groupName, remoteFilename, err := splitFileId(fileId)
	if err != nil {
		return err
	}
	storageInfo, err := c.queryStorageInfoWithTracker(TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE, groupName, remoteFilename)
	if err != nil {
		return err
	}

	task := &storageDownloadTask{}
	//req
	task.groupName = groupName
	task.remoteFilename = remoteFilename
	task.offset = offset
	task.downloadBytes = downloadBytes
	task.buffer = buffer //allocate buffer by user

	//res
	if err := c.doStorage(task, storageInfo); err != nil {
		return err
	}
	return nil
}

func (c *Client) DeleteFile(fileId string) error {
	groupName, remoteFilename, err := splitFileId(fileId)
	if err != nil {
		return err
	}
	storageInfo, err := c.queryStorageInfoWithTracker(TRACKER_PROTO_CMD_SERVICE_QUERY_FETCH_ONE, groupName, remoteFilename)
	if err != nil {
		return err
	}

	task := &storageDeleteTask{}
	//req
	task.groupName = groupName
	task.remoteFilename = remoteFilename

	return c.doStorage(task, storageInfo)
}

func (c *Client) doTracker(task task) error {
	trackerConn, err := c.getTrackerConn()
	if err != nil {
		return err
	}
	defer trackerConn.Close()

	if err := task.SendReq(trackerConn); err != nil {
		return err
	}
	if err := task.RecvRes(trackerConn); err != nil {
		return err
	}

	return nil
}

func (c *Client) doStorage(task task, storageInfo *storageInfo) error {
	storageConn, err := c.getStorageConn(storageInfo)
	if err != nil {
		return err
	}
	defer storageConn.Close()

	if err := task.SendReq(storageConn); err != nil {
		return err
	}
	if err := task.RecvRes(storageConn); err != nil {
		return err
	}

	return nil
}

func (c *Client) queryStorageInfoWithTracker(cmd int8, groupName string, remoteFilename string) (*storageInfo, error) {
	task := &trackerTask{}
	task.cmd = cmd
	task.groupName = groupName
	task.remoteFilename = remoteFilename

	if err := c.doTracker(task); err != nil {
		return nil, err
	}
	return &storageInfo{
		addr:             fmt.Sprintf("%s:%d", task.ipAddr, task.port),
		storagePathIndex: task.storePathIndex,
	}, nil
}

func (c *Client) getTrackerConn() (net.Conn, error) {
	var trackerConn net.Conn
	var err error
	var getOne bool
	for _, trackerPool := range c.trackerPools {
		trackerConn, err = trackerPool.get()
		if err == nil {
			getOne = true
			break
		}
	}
	if getOne {
		return trackerConn, nil
	}
	if err == nil {
		return nil, fmt.Errorf("no connPool can be use")
	}
	return nil, err
}

func (c *Client) getStorageConn(storageInfo *storageInfo) (net.Conn, error) {
	c.storagePoolLock.Lock()
	storagePool, ok := c.storagePools[storageInfo.addr]
	if ok {
		c.storagePoolLock.Unlock()
		return storagePool.get()
	}
	storagePool, err := newConnPool(storageInfo.addr, c.config.maxConns)
	if err != nil {
		c.storagePoolLock.Unlock()
		return nil, err
	}
	c.storagePools[storageInfo.addr] = storagePool
	c.storagePoolLock.Unlock()
	return storagePool.get()
}
