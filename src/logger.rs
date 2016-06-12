use log::*;
use std::io::{ stdout, stderr, Write };

pub struct StdLogger;

impl Log for StdLogger {
    fn enabled(&self, _: &LogMetadata) -> bool {
        true
    }

    fn log(&self, record: &LogRecord) {
        if record.metadata().level() >= LogLevel::Warn {
            writeln!(&mut stderr(), "[{}] {}", record.level(), record.args()).unwrap();
        } else {
            writeln!(&mut stdout(), "[{}] {}", record.level(), record.args()).unwrap();
        }
    }
}

impl StdLogger {
    pub fn init(opt: Option<LogLevelFilter>) -> Result<(), SetLoggerError> {
        set_logger(|max_log_level| {
            max_log_level.set(opt.unwrap_or(LogLevelFilter::Info));
            Box::new(StdLogger)
        })
    }
}
