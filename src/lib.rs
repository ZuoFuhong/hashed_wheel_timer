use std::cell::{RefCell, RefMut};
use std::error::Error;
use std::ops::Deref;
use std::rc::Rc;
use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::mpsc;
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, SystemTime};

const WORKER_STATE_INIT: u8 = 0;
const WORKER_STATE_STARTED: u8 = 1;
const WORKER_STATE_SHUTDOWN: u8 = 2;

pub struct WheelTimer {
    worker_state: Arc<AtomicU8>, // 0 - init, 1 - started, 2 - shutdown
    start_time: u64,
    tick_duration: u64, // the duration between tick, time unit is millisecond
    ticks_per_wheel: u32,
    mask: u64,
    condvar: Arc<(Mutex<u64>, Condvar)>,
    sender: Option<Sender<WheelTimeout>>,
}

impl WheelTimer {
    pub fn new(tick_duration: u64, ticks_per_wheel: u32) -> Result<WheelTimer, Box<dyn Error>> {
        if tick_duration <= 0 {
            return Err(format!("tickDuration must be greater than 0: {}", tick_duration).into());
        }
        if ticks_per_wheel <= 0 {
            return Err(
                format!("ticksPerWheel must be greater than 0: {}", ticks_per_wheel).into(),
            );
        }
        if ticks_per_wheel > 1073741824 {
            return Err(format!(
                "ticksPerWheel may not be greater than 2^30: {}",
                ticks_per_wheel
            )
                .into());
        }
        let ticks_per_wheel = normalize_ticks_per_wheel(ticks_per_wheel);
        let mask = (ticks_per_wheel - 1) as u64;

        // Prevent overflow
        if tick_duration >= u64::MAX / ticks_per_wheel as u64 {
            return Err(format!(
                "tickDuration: {} (expected: 0 < tickDuration in nanos < {}",
                tick_duration,
                u64::MAX / ticks_per_wheel as u64
            )
                .into());
        }
        let worker_state = Arc::new(AtomicU8::new(WORKER_STATE_INIT));
        let condvar = Arc::new((Mutex::new(0), Condvar::new()));

        Ok(WheelTimer {
            worker_state,
            start_time: 0,
            tick_duration,
            ticks_per_wheel,
            mask,
            condvar,
            sender: None,
        })
    }

    pub fn start(&mut self) -> Result<(), Box<dyn Error + '_>> {
        match self.worker_state.load(Ordering::SeqCst) {
            WORKER_STATE_INIT => {
                let ret = self.worker_state.compare_exchange(
                    WORKER_STATE_INIT,
                    WORKER_STATE_STARTED,
                    Ordering::SeqCst,
                    Ordering::Acquire,
                );
                match ret {
                    Ok(_) => {
                        let (tx, rx) = mpsc::channel();
                        self.sender = Some(tx);
                        let worker_state = self.worker_state.clone();
                        let condvar = self.condvar.clone();
                        let tick_duration = self.tick_duration;
                        let mask = self.mask;
                        let ticks_per_wheel = self.ticks_per_wheel;

                        thread::spawn(move || {
                            let mut worker = Worker::new(
                                worker_state,
                                condvar,
                                tick_duration,
                                mask,
                                ticks_per_wheel,
                                rx,
                            );
                            worker.start();
                        });
                    }
                    Err(_) => {
                        // nothing to do
                    }
                }
            }
            WORKER_STATE_STARTED => {
                // nothing to do
            }
            WORKER_STATE_SHUTDOWN => return Err("cannot be started once stopped".into()),
            _ => return Err("Invalid worker state".into()),
        }
        // Wait worker thread initialize start_time finish
        let (lock, condvar) = self.condvar.deref();
        let mut guard = lock.lock()?;
        while *guard == 0 {
            guard = condvar.wait(guard)?;
            self.start_time = *guard;
        }
        Ok(())
    }

    pub fn stop(&self) {
        let ret = self.worker_state.compare_exchange(
            WORKER_STATE_STARTED,
            WORKER_STATE_SHUTDOWN,
            Ordering::SeqCst,
            Ordering::Acquire,
        );
        match ret {
            Ok(_) => {
                // releases resources
            }
            Err(_) => {
                self.worker_state
                    .swap(WORKER_STATE_SHUTDOWN, Ordering::SeqCst);
            }
        }
    }

    pub fn new_timeout(&mut self, task: Box<dyn TimerTask + Send>, delay: Duration) {
        self.start().unwrap();

        let deadline = system_time_unix() + delay.as_millis() as u64 - self.start_time;
        let timeout = WheelTimeout::new(task, deadline);
        let sender = self.sender.as_ref().unwrap();
        sender.send(timeout).unwrap();
    }
}

fn normalize_ticks_per_wheel(ticks_per_wheel: u32) -> u32 {
    let mut normalized_ticks_per_wheel = 1;
    while normalized_ticks_per_wheel < ticks_per_wheel {
        normalized_ticks_per_wheel <<= 1;
    }
    normalized_ticks_per_wheel
}

struct Worker {
    worker_state: Arc<AtomicU8>,
    condvar: Arc<(Mutex<u64>, Condvar)>,
    tick: u64,
    tick_duration: u64,
    mask: u64,
    wheel: Vec<WheelBucket>,
    start_time: u64,
    receiver: Receiver<WheelTimeout>,
    last_task_id: AtomicU64,
}

impl Worker {
    fn new(
        worker_state: Arc<AtomicU8>,
        condvar: Arc<(Mutex<u64>, Condvar)>,
        tick_duration: u64,
        mask: u64,
        ticks_per_wheel: u32,
        rx: Receiver<WheelTimeout>,
    ) -> Worker {
        let wheel = create_wheel(ticks_per_wheel).unwrap();

        Worker {
            worker_state,
            condvar,
            tick: 0,
            tick_duration,
            mask,
            wheel,
            start_time: 0,
            receiver: rx,
            last_task_id: AtomicU64::new(1),
        }
    }

    fn start(&mut self) {
        // Initialize the startTime.
        let mut start_time = system_time_unix();
        if start_time == 0 {
            start_time = 1;
        }
        self.start_time = start_time;

        // Notify the other thread waiting for the initialization at start()
        let (lock, condvar) = self.condvar.deref();
        let mut guard = lock.lock().unwrap();
        *guard = start_time;
        condvar.notify_one();
        drop(guard);

        while self.worker_state.load(Ordering::SeqCst) == WORKER_STATE_STARTED {
            let deadline = self.wait_for_next_tick();
            if deadline > 0 {
                self.transfer_timeouts_to_buckets();
                let idx = self.tick & self.mask;
                let bucket = self.wheel.get_mut(idx as usize).unwrap();
                bucket.expire_timeouts(deadline);
                self.tick += 1;
            }
        }
        println!("Worker shutdown")
    }

    fn wait_for_next_tick(&self) -> u64 {
        let deadline = (self.tick_duration * (self.tick + 1)) as i64;
        loop {
            let current_time = (system_time_unix() - self.start_time) as i64;
            let sleep_time_ms = (deadline - current_time + 999999) / 1000000;

            if sleep_time_ms <= 0 {
                return current_time as u64;
            }
            thread::sleep(Duration::new(0, (sleep_time_ms * 1000000) as u32));
        }
    }

    fn transfer_timeouts_to_buckets(&mut self) {
        for _ in 0..100000 {
            match self.receiver.try_recv() {
                Ok(timeout) => {
                    let task_id = self.last_task_id.fetch_add(1, Ordering::SeqCst);
                    let calculated = timeout.deadline / self.tick_duration;

                    let mut bucket_timeout =
                        BucketTimeout::new(task_id, timeout.task, timeout.deadline);
                    bucket_timeout.remaining_rounds =
                        (calculated - self.tick) / self.wheel.len() as u64;

                    let mut ticks = self.tick;
                    if calculated > self.tick {
                        ticks = calculated;
                    }
                    let stop_index = ticks & self.mask;

                    let bucket = self.wheel.get_mut(stop_index as usize).unwrap();
                    bucket.add_timeout(bucket_timeout);
                }
                Err(_) => {
                    break;
                }
            }
        }
    }
}

fn create_wheel(ticks_per_wheel: u32) -> Result<Vec<WheelBucket>, Box<dyn Error>> {
    let mut wheel = Vec::with_capacity(ticks_per_wheel as usize);
    for _ in 0..ticks_per_wheel {
        wheel.push(WheelBucket {
            head: None,
            tail: None,
        })
    }
    Ok(wheel)
}

fn system_time_unix() -> u64 {
    SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

pub struct WheelBucket {
    head: Option<Rc<RefCell<BucketTimeout>>>,
    tail: Option<Rc<RefCell<BucketTimeout>>>,
}

impl WheelBucket {
    fn add_timeout(&mut self, mut timeout: BucketTimeout) {
        println!(
            "WheelBucket add_timeout, task_id = {} deadline = {}",
            timeout.task_id, timeout.deadline
        );
        match self.head.as_ref() {
            None => {
                let rc_timeout = Rc::new(RefCell::new(timeout));
                self.head = Some(rc_timeout.clone());
                self.tail = Some(rc_timeout);
            }
            Some(_) => {
                let rc_tail = self.tail.as_ref().unwrap().clone();
                timeout.prev = Some(rc_tail);
                let rc_timeout = Rc::new(RefCell::new(timeout));
                {
                    let mut tail = self.tail.as_ref().unwrap().deref().borrow_mut();
                    tail.next = Some(rc_timeout.clone());
                }
                self.tail = Some(rc_timeout);
            }
        }
    }

    fn expire_timeouts(&mut self, deadline: u64) {
        let mut current = self.head.clone();
        loop {
            match current {
                None => {
                    return;
                }
                Some(timeout) => {
                    let mut next = RefCell::borrow(&timeout).next.clone();

                    let mut timeout_mut = RefCell::borrow_mut(&timeout);
                    if timeout_mut.remaining_rounds <= 0 {
                        next = self.remove(timeout_mut);

                        let mut timeout_mut = RefCell::borrow_mut(&timeout);
                        timeout_mut.prev = None;
                        timeout_mut.next = None;
                        if timeout_mut.deadline <= deadline {
                            timeout_mut.expire();
                        } else {
                            // The timeout was placed into a wrong slot. This should never happen.
                            panic!(
                                "timeout.deadline {} > deadline {}",
                                timeout_mut.deadline, deadline
                            )
                        }
                    } else if timeout_mut.is_cancelled() {
                        next = self.remove(timeout_mut);
                    } else {
                        timeout_mut.remaining_rounds -= 1;
                    }
                    current = next;
                }
            }
        }
    }

    fn remove(&mut self, timeout: RefMut<BucketTimeout>) -> Option<Rc<RefCell<BucketTimeout>>> {
        let prev = timeout.prev.clone();
        let next = timeout.next.clone();
        match prev.clone() {
            None => {}
            Some(v) => {
                let mut prev = v.deref().borrow_mut();
                prev.next = next.clone();
            }
        }
        match next.clone() {
            None => {}
            Some(v) => {
                let mut next = v.deref().borrow_mut();
                next.prev = prev.clone()
            }
        }
        let task_id = timeout.task_id;
        // release borrow
        drop(timeout);

        let head_task_id = self.head.as_ref().unwrap().deref().borrow().task_id;
        let tail_task_id = self.tail.as_ref().unwrap().deref().borrow().task_id;
        if task_id == head_task_id {
            if task_id == tail_task_id {
                self.tail = None;
                self.head = None;
            } else {
                self.head = next.clone()
            }
        } else if task_id == tail_task_id {
            self.tail = prev.clone();
        }
        next
    }
}

const ST_INIT: u8 = 0;
const ST_CANCELLED: u8 = 1;
const ST_EXPIRED: u8 = 2;

struct BucketTimeout {
    task_id: u64,
    state: AtomicU8, // 0: init, 1: cancelled, 2: expired
    deadline: u64,
    remaining_rounds: u64,
    task: Box<dyn TimerTask + Send>,
    prev: Option<Rc<RefCell<BucketTimeout>>>,
    next: Option<Rc<RefCell<BucketTimeout>>>,
}

impl BucketTimeout {
    fn new(task_id: u64, task: Box<dyn TimerTask + Send>, deadline: u64) -> BucketTimeout {
        BucketTimeout {
            task_id,
            state: AtomicU8::new(ST_INIT),
            deadline,
            task,
            remaining_rounds: 0,
            prev: None,
            next: None,
        }
    }

    fn state(&self) -> u8 {
        self.state.load(Ordering::SeqCst)
    }

    fn is_cancelled(&self) -> bool {
        self.state() == ST_CANCELLED
    }

    fn compare_exchange(&self, expected: u8, state: u8) -> bool {
        let ret =
            self.state
                .compare_exchange(expected, state, Ordering::SeqCst, Ordering::Acquire);
        match ret {
            Err(_) => {
                false
            }
            Ok(_) => {
                true
            }
        }
    }

    fn expire(&self) {
        if !self.compare_exchange(ST_INIT, ST_EXPIRED) {
            return;
        }
        self.task.run();
    }
}

pub trait TimerTask {
    fn run(&self);
}

struct WheelTimeout {
    deadline: u64,
    task: Box<dyn TimerTask + Send>,
}

impl WheelTimeout {
    fn new(task: Box<dyn TimerTask + Send>, deadline: u64) -> WheelTimeout {
        WheelTimeout { deadline, task }
    }
}
