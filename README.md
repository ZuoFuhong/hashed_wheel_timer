# HashedWheelTimer

Rust implementation of the Netty HashedWheelTimer.

## Build requirements

You only need a stable version of the Rust compiler.

## How to use the library

Put the following in your `Cargo.toml`:

```toml
[dependencies]
hashed_wheel_timer = "0.1.0"
```

### Example

The following bit of code will write the byte string to STDOUT

```rust
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
    fn run(&self) {
        println!("ReaderIdleTimeoutTask expire, value {}", self.value)
    }
}

fn main() {
    let mut timer = WheelTimer::new(500, 10).unwrap();
    let task = Box::new(ReaderIdleTimeoutTask::new(100));
    timer.new_timeout(task, Duration::new(1, 0));

    thread::sleep(Duration::new(5, 0))
}
```