use std::time::{SystemTime, UNIX_EPOCH};

use agent_broker_domain::TimestampMs;

use crate::RuntimeError;

pub(crate) fn system_clock_ms() -> Result<TimestampMs, RuntimeError> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| RuntimeError::ClockBeforeUnixEpoch)?;
    let millis = u64::try_from(duration.as_millis()).map_err(|_| {
        RuntimeError::InvalidConfiguration("system clock millisecond value exceeds u64")
    })?;
    Ok(TimestampMs::new(millis))
}
