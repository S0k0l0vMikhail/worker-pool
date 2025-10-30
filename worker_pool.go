package worker_pool

import (
	"sync"
)

type WorkerPool struct {
	maxTasks   int
	taskQueue  []func() // слайс для обычных задач
	waitQueue  []func() // слайс для задач с ожиданием
	stop       bool     // не начинать вполнять задачи из очереди
	stopping   bool     // не добавлять задачи в очереди
	mutex      sync.Mutex
	cond       *sync.Cond
	waitCond   *sync.Cond
	waitGroup  sync.WaitGroup
	waitGroup2 sync.WaitGroup
}

func NewWorkerPool(numberOfWorkers int) *WorkerPool {
	wp := &WorkerPool{
		maxTasks:  numberOfWorkers,
		taskQueue: make([]func(), 0),
		waitQueue: make([]func(), 0),
		stop:      false,
		stopping:  false,
	}

	wp.cond = sync.NewCond(&wp.mutex)
	wp.waitCond = sync.NewCond(&wp.mutex)

	// Запускаем воркеров для обычных задач
	for i := 0; i < wp.maxTasks; i++ {
		wp.waitGroup.Add(1)
		go wp.worker()
	}

	// Запускаем отдельного воркера для задач с ожиданием
	wp.waitGroup2.Add(1)
	go wp.waitWorker()

	return wp
}

// worker - воркер для обработки обычных задач
func (wp *WorkerPool) worker() {
	defer wp.waitGroup.Done()

	for {
		wp.mutex.Lock()

		// Ждем пока появятся задачи или пул не остановят
		for len(wp.taskQueue) == 0 && !wp.stop {
			wp.cond.Wait()
		}

		// Если пул остановлен и задач нет - выходим
		if wp.stop && len(wp.taskQueue) == 0 {
			wp.mutex.Unlock()
			return
		}

		// Берем первую задачу из слайса
		if len(wp.taskQueue) > 0 {
			task := wp.taskQueue[0]
			// Удаляем задачу из слайса
			wp.taskQueue = wp.taskQueue[1:]
			wp.mutex.Unlock()

			// Выполняем задачу
			task()
		} else {
			wp.mutex.Unlock()
		}
	}
}

// waitWorker - воркер для обработки задач с ожиданием
func (wp *WorkerPool) waitWorker() {
	defer wp.waitGroup2.Done()

	for {
		wp.mutex.Lock()

		// Ждем пока появятся задачи или пул не остановят
		for len(wp.waitQueue) == 0 && !wp.stop {
			wp.waitCond.Wait()
		}

		// Если пул остановлен и задач нет - выходим
		if wp.stop && len(wp.waitQueue) == 0 {
			wp.mutex.Unlock()
			return
		}

		// Берем первую задачу из слайса
		if len(wp.waitQueue) > 0 {
			task := wp.waitQueue[0]
			// Удаляем задачу из слайса
			wp.waitQueue = wp.waitQueue[1:]
			wp.mutex.Unlock()

			// Выполняем задачу
			task()
		} else {
			wp.mutex.Unlock()
		}
	}
}

// Submit - добавить таску в воркер пул
func (wp *WorkerPool) Submit(task func()) {
	wp.mutex.Lock()
	defer wp.mutex.Unlock()

	if wp.stop || wp.stopping {
		return
	}

	wp.taskQueue = append(wp.taskQueue, task)
	wp.cond.Signal() // Будим одного воркера
}

// SubmitWait - добавить таску в воркер пул и дождаться окончания ее выполнения
func (wp *WorkerPool) SubmitWait(task func()) {
	wp.mutex.Lock()
	if wp.stop || wp.stopping {
		wp.mutex.Unlock()
		return
	}
	wp.mutex.Unlock()

	done := make(chan struct{})

	wp.mutex.Lock()
	wp.waitQueue = append(wp.waitQueue, func() {
		task()
		close(done)
	})
	wp.waitCond.Signal()
	wp.mutex.Unlock()

	<-done
}

// Stop - остановить воркер пул, дождаться выполнения только тех тасок, которые выполняются сейчас
func (wp *WorkerPool) Stop() {
	wp.mutex.Lock()
	if wp.stop {
		wp.mutex.Unlock()
		return
	}
	wp.stop = true
	wp.mutex.Unlock()

	// Будим всех воркеров чтобы они могли завершиться
	wp.cond.Broadcast()
	wp.waitCond.Signal()

	// Ждем только завершения текущих воркеров
	wp.waitGroup.Wait()
	wp.waitGroup2.Wait()
}

// StopWait - остановить воркер пул, дождаться выполнения всех тасок, даже тех, что не начали выполняться, но лежат в очереди
func (wp *WorkerPool) StopWait() {
	wp.mutex.Lock()
	if wp.stopping || wp.stop {
		wp.mutex.Unlock()
		return
	}
	wp.stopping = true
	wp.mutex.Unlock()

	// Выполняем все задачи из обычной очереди
	for {
		wp.mutex.Lock()
		if len(wp.taskQueue) == 0 {
			wp.mutex.Unlock()
			break
		}

		// Берем первую задачу
		task := wp.taskQueue[0]
		wp.taskQueue = wp.taskQueue[1:]
		wp.mutex.Unlock()

		task()
	}

	// Выполняем все задачи из очереди ожидания
	for {
		wp.mutex.Lock()
		if len(wp.waitQueue) == 0 {
			wp.mutex.Unlock()
			break
		}

		// Берем первую задачу
		task := wp.waitQueue[0]
		wp.waitQueue = wp.waitQueue[1:]
		wp.mutex.Unlock()

		task()
	}

	// Останавливаем воркеров
	wp.mutex.Lock()
	wp.stop = true
	wp.stopping = false
	wp.mutex.Unlock()

	// Будим всех воркеров чтобы они могли завершиться
	wp.cond.Broadcast()
	wp.waitCond.Signal()

	// Ждем завершения всех воркеров
	wp.waitGroup.Wait()
	wp.waitGroup2.Wait()
}
