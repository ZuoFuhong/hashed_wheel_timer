use hashed_wheel_timer::{TimerTask, WheelTimer};
use std::thread;
use std::time::Duration;

struct ReaderIdleTimeoutTask {
    value: u64,
}

impl ReaderIdleTimeoutTask {
    fn new(value: u64) -> ReaderIdleTimeoutTask {
        ReaderIdleTimeoutTask { value }
    }
}

impl TimerTask for ReaderIdleTimeoutTask {
    fn run(&mut self) {
        println!("ReaderIdleTimeoutTask expire, value {}", self.value)
    }
}

fn main() {
    let mut timer = WheelTimer::new(500, 10).unwrap();
    let task = Box::new(ReaderIdleTimeoutTask::new(100));
    timer.new_timeout(task, Duration::new(1, 0));

    thread::sleep(Duration::new(5, 0))
}

