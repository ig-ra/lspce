use once_cell::sync::Lazy;
use std::{
    fs::File,
    io::{Error, Write},
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc, Mutex,
    },
};

use chrono::Local;
use lazy_static::lazy_static;

pub const LOG_DISABLED: u8 = 0;
pub const LOG_ERROR: u8 = 1;
pub const LOG_INFO: u8 = 2;
pub const LOG_TRACE: u8 = 3;
pub const LOG_DEBUG: u8 = 4;

pub static LOG_LEVEL: AtomicU8 = AtomicU8::new(LOG_INFO);

lazy_static! {
    pub static ref LOG_FILE_NAME: Mutex<String> = Mutex::new(String::new());
}

pub fn log_enabled(level: u8) -> bool {
    LOG_LEVEL.load(Ordering::Relaxed) >= level
}

pub fn log_file_name() -> String {
    LOG_FILE_NAME.lock().unwrap().clone()
}

pub struct Logger {}

impl Logger {
    fn log(buf: impl AsRef<str>) {
        let mut logger = logger().lock().unwrap();
        logger.write_all(Local::now().format("%Y-%m-%d %H:%M:%S%.3f - ").to_string().as_bytes());
        logger.write_all(buf.as_ref().as_bytes());
        logger.write_all("\n".as_bytes());
    }

    fn log_if_enabled(level: u8, buf: impl AsRef<str>) {
        let cur_level = LOG_LEVEL.load(Ordering::Relaxed);
        if cur_level >= level {
            Logger::log(buf);
        }
    }

    pub fn error(buf: impl AsRef<str>) {
        Logger::log_if_enabled(LOG_ERROR, buf);
    }
    pub fn info(buf: impl AsRef<str>) {
        Logger::log_if_enabled(LOG_INFO, buf);
    }
    pub fn trace(buf: impl AsRef<str>) {
        Logger::log_if_enabled(LOG_TRACE, buf);
    }
    pub fn debug(buf: impl AsRef<str>) {
        Logger::log_if_enabled(LOG_DEBUG, buf);
    }
}

struct FakeFile {}

impl Write for FakeFile {
    fn write(&mut self, buf: &[u8]) -> Result<usize, Error> {
        Ok(buf.len())
    }
    fn flush(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

static LOGGER: Lazy<Arc<Mutex<dyn Write + Send>>> = Lazy::new(|| {
    let file_name = log_file_name();
    if !file_name.is_empty() {
        if let Ok(f) = File::options().create(true).append(true).open(file_name) {
            return Arc::new(Mutex::new(f));
        }
    }
    Arc::new(Mutex::new(FakeFile {}))
});

fn logger() -> &'static Arc<Mutex<dyn Write + Send>> {
    &LOGGER
}
