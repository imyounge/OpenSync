package service

import (
	"log"
	"strings"
	"time"
)

// persistBusyRetryDelays 是数据库写锁冲突时的重试退避。
// 多个任务并发运行时，各任务的落库缓冲会周期性重叠；SQLite 是单写者模型，
// 短暂写锁冲突不应该让整个任务失败，因此这里做有限次退避重试。
var persistBusyRetryDelays = []time.Duration{
	200 * time.Millisecond,
	1 * time.Second,
	3 * time.Second,
}

// isSQLiteBusyError 判断错误是否为 SQLite 写锁竞争（可重试）。
func isSQLiteBusyError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, marker := range []string{
		"sqlite_busy",
		"sqlite_locked",
		"database is locked",
		"database table is locked",
	} {
		if strings.Contains(msg, marker) {
			return true
		}
	}
	return false
}

// persistJobTaskItemsWithRetry 在遇到写锁冲突时退避重试，其它错误立即返回。
func persistJobTaskItemsWithRetry(items []map[string]interface{}) error {
	err := persistJobTaskItems(items)
	for _, delay := range persistBusyRetryDelays {
		if err == nil || !isSQLiteBusyError(err) {
			return err
		}
		log.Printf("Database is busy while saving %d task items, retrying in %s: %v", len(items), delay, err)
		time.Sleep(delay)
		err = persistJobTaskItems(items)
	}
	return err
}

func (jt *JobTask) persistRemainingTaskItems() error {
	if err := jt.flushPersistBuffer(); err != nil {
		return err
	}
	return jt.taskPersistenceError()
}

func (jt *JobTask) appendFinish(item JobTaskItem) {
	jt.appendFinishMany([]JobTaskItem{item})
}

func (jt *JobTask) appendFinishMany(items []JobTaskItem) {
	if len(items) == 0 {
		return
	}
	jt.initRuntime()

	jt.FinishMu.Lock()
	for _, item := range items {
		status := item.Status
		jt.FinishedCounts[status]++
		if size := item.CountableFileSize(); size > 0 {
			jt.FinishedSizes[status] += size
		}
	}
	jt.FinishMu.Unlock()

	jt.persistBufMu.Lock()
	jt.persistBuffer = append(jt.persistBuffer, items...)
	needFlush := len(jt.persistBuffer) >= persistBatchSize
	jt.persistBufMu.Unlock()

	if needFlush {
		if err := jt.flushPersistBuffer(); err != nil {
			jt.recordTaskPersistenceError(err)
			jt.requestBreak()
		}
	} else {
		jt.schedulePersistFlush()
	}
	jt.notifyProgressChange()
}

func (jt *JobTask) schedulePersistFlush() {
	jt.persistFlushMu.Lock()
	defer jt.persistFlushMu.Unlock()
	if jt.persistFlushScheduled {
		return
	}
	jt.persistFlushScheduled = true
	time.AfterFunc(persistFlushInterval, func() {
		jt.persistFlushMu.Lock()
		jt.persistFlushScheduled = false
		jt.persistFlushMu.Unlock()
		if err := jt.flushPersistBuffer(); err != nil {
			jt.recordTaskPersistenceError(err)
			jt.requestBreak()
		}
	})
}

func (jt *JobTask) flushPersistBuffer() error {
	jt.persistBufMu.Lock()
	if len(jt.persistBuffer) == 0 {
		jt.persistBufMu.Unlock()
		return jt.taskPersistenceError()
	}
	items := append([]JobTaskItem(nil), jt.persistBuffer...)
	jt.persistBuffer = jt.persistBuffer[:0]
	jt.persistBufMu.Unlock()

	if err := persistJobTaskItemsWithRetry(jobTaskItemsToMaps(items)); err != nil {
		jt.persistBufMu.Lock()
		jt.persistBuffer = append(items, jt.persistBuffer...)
		jt.persistBufMu.Unlock()

		jobID := int64(0)
		if jt.JobClient != nil {
			jobID = jt.JobClient.JobID
		}
		first := items[0]
		log.Printf(
			"Failed to save %d task items for task %d (job %d) first file=%q: %v",
			len(items), jt.TaskID, jobID, first.FileName, err,
		)
		jt.recordTaskPersistenceError(err)
		jt.requestBreak()
		return err
	}
	return jt.taskPersistenceError()
}

func (jt *JobTask) recordTaskPersistenceError(err error) {
	if err == nil {
		return
	}
	jt.PersistMu.Lock()
	defer jt.PersistMu.Unlock()
	if jt.PersistErr == nil {
		jt.PersistErr = err
	}
}

func (jt *JobTask) taskPersistenceError() error {
	jt.PersistMu.Lock()
	defer jt.PersistMu.Unlock()
	return jt.PersistErr
}
