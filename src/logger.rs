use std::{
    fs::File,
    io::{Error, Write},
    sync::{
        atomic::{AtomicU8, Ordering},
        Arc, LazyLock, Mutex,
    },
};

use time::{macros::format_description, OffsetDateTime};

pub const LOG_DISABLED: u8 = 0;
pub const LOG_ERROR: u8 = 1;
pub const LOG_INFO: u8 = 2;
pub const LOG_TRACE: u8 = 3;
pub const LOG_DEBUG: u8 = 4;

pub static LOG_LEVEL: AtomicU8 = AtomicU8::new(LOG_INFO);

pub static LOG_FILE_NAME: LazyLock<Mutex<String>> = LazyLock::new(|| Mutex::new(String::new()));

pub fn log_enabled(level: u8) -> bool {
    LOG_LEVEL.load(Ordering::Relaxed) >= level
}

pub fn enable_logging() {
    LOG_LEVEL.store(LOG_DEBUG, Ordering::Relaxed);
}
pub fn disable_logging() {
    LOG_LEVEL.store(LOG_DISABLED, Ordering::Relaxed);
}

pub fn log_file_name() -> String {
    LOG_FILE_NAME.lock().unwrap().clone()
}

pub fn set_log_file_name(file_name: String) {
    *LOG_FILE_NAME.lock().unwrap() = file_name;
}

pub fn set_log_level(level: u8) {
    LOG_LEVEL.store(level, Ordering::Relaxed);
}

pub fn get_log_level() -> u8 {
    LOG_LEVEL.load(Ordering::Relaxed)
}

pub struct Logger {}

impl Logger {
    fn log(buf: impl AsRef<str>) {
        let mut logger = logger().lock().unwrap();

        // const/compile-time format string
        const FORMAT: &[time::format_description::FormatItem] =
            format_description!("[year]-[month]-[day] [hour]:[minute]:[second].[subsecond digits:3] - ");

        let now = OffsetDateTime::now_local().unwrap_or_else(|_| OffsetDateTime::now_utc());
        let timestamp = now.format(&FORMAT).unwrap_or_else(|_| "timestamp-error - ".to_string());

        logger.write_all(timestamp.as_bytes());
        logger.write_all(buf.as_ref().as_bytes());
        logger.write_all(b"\n");
        logger.flush();
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

static LOGGER: LazyLock<Arc<Mutex<dyn Write + Send>>> = LazyLock::new(|| {
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
