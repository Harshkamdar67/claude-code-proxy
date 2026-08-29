use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

pub const RETRY_INITIAL_DELAY_MS: u64 = 2000;
pub const RETRY_MAX_DELAY_MS: u64 = 30_000;
pub const RETRY_BACKOFF_FACTOR: u64 = 2;
pub const MAX_RATE_LIMIT_RETRIES: u32 = 3;

#[derive(Debug, Clone, Copy)]
pub struct BackoffOutcome {
    pub wait_ms: u64,
    pub exceeds_budget: bool,
}

/// How many retries a rate limit is worth before it is surfaced.
///
/// Retrying is right when this proxy is the only thing between the client
/// and the provider. It is wrong when the caller has its own failover: a
/// session router with a healthy alternative model already chosen pays the
/// whole backoff for nothing, and the user waits minutes for a switch that
/// was ready immediately. `CCP_MAX_RATE_LIMIT_RETRIES` lets such a caller
/// lower the budget. Absent, unparsable, or larger than the default leaves
/// the previous behaviour untouched.
pub fn rate_limit_retry_budget(default_retries: u32) -> u32 {
    parse_rate_limit_budget(
        std::env::var("CCP_MAX_RATE_LIMIT_RETRIES").ok().as_deref(),
        default_retries,
    )
}

/// The parsing half, kept separate so it can be tested without the process
/// environment, which tests share.
pub fn parse_rate_limit_budget(raw: Option<&str>, default_retries: u32) -> u32 {
    raw.and_then(|value| value.trim().parse::<u32>().ok())
        .map(|value| value.min(default_retries))
        .unwrap_or(default_retries)
}

/// True for the statuses that mean "busy", as opposed to "broken".
pub fn is_rate_limit_status(status: u16) -> bool {
    matches!(status, 429 | 529)
}

pub fn should_retry_status(status: u16) -> bool {
    matches!(status, 429 | 500 | 502 | 503 | 504)
}

pub fn compute_backoff_delay(attempt: u32, retry_after: Option<&str>) -> BackoffOutcome {
    if let Some(raw) = retry_after
        && let Ok(raw_secs) = raw.parse::<f64>()
    {
        let target_ms = (raw_secs * 1000.0).ceil() as u64;
        return BackoffOutcome {
            wait_ms: target_ms.min(RETRY_MAX_DELAY_MS),
            exceeds_budget: target_ms > RETRY_MAX_DELAY_MS,
        };
    }

    let mut exp =
        RETRY_INITIAL_DELAY_MS.saturating_mul(RETRY_BACKOFF_FACTOR.saturating_pow(attempt));
    if exp > RETRY_MAX_DELAY_MS {
        exp = RETRY_MAX_DELAY_MS;
    }
    let jitter = exp / 2;
    let wait_ms = (exp / 2) + (jitter / 2);
    BackoffOutcome {
        wait_ms,
        exceeds_budget: false,
    }
}

static ZERO_RETRY_DELAY_FOR_TESTS: AtomicBool = AtomicBool::new(false);

/// Make retry sleeps return immediately so exhaustion paths can be exercised
/// in tests without waiting out the real backoff schedule.
pub fn set_zero_retry_delay_for_tests(enabled: bool) {
    ZERO_RETRY_DELAY_FOR_TESTS.store(enabled, Ordering::SeqCst);
}

pub async fn sleep(ms: u64) {
    if ZERO_RETRY_DELAY_FOR_TESTS.load(Ordering::SeqCst) {
        return;
    }
    tokio::time::sleep(Duration::from_millis(ms)).await;
}

#[cfg(test)]
pub async fn retry_on_statuses<T, E, F>(mut next: F) -> Result<T, E>
where
    E: std::fmt::Debug,
    F: FnMut(u32) -> Result<T, E>,
{
    let mut attempt = 0;
    loop {
        attempt += 1;
        if attempt > MAX_RATE_LIMIT_RETRIES + 1 {
            break;
        }
        match next(attempt) {
            Ok(value) => return Ok(value),
            Err(err) if attempt <= MAX_RATE_LIMIT_RETRIES + 1 => {
                if attempt > MAX_RATE_LIMIT_RETRIES {
                    return Err(err);
                }
                sleep(compute_backoff_delay(attempt, None).wait_ms).await;
            }
            Err(err) => return Err(err),
        }
    }
    unreachable!()
}

#[cfg(test)]
mod rate_limit_budget_tests {
    use super::{is_rate_limit_status, parse_rate_limit_budget};

    #[test]
    fn absent_or_unparsable_keeps_the_default() {
        assert_eq!(parse_rate_limit_budget(None, 3), 3);
        assert_eq!(parse_rate_limit_budget(Some(""), 3), 3);
        assert_eq!(parse_rate_limit_budget(Some("not a number"), 3), 3);
        assert_eq!(parse_rate_limit_budget(Some("-1"), 3), 3);
    }

    #[test]
    fn a_caller_with_its_own_failover_can_opt_out() {
        assert_eq!(parse_rate_limit_budget(Some("0"), 3), 0);
        assert_eq!(parse_rate_limit_budget(Some(" 1 "), 3), 1);
    }

    #[test]
    fn it_can_only_lower_the_budget_never_raise_it() {
        assert_eq!(parse_rate_limit_budget(Some("99"), 3), 3);
    }

    #[test]
    fn only_busy_statuses_count_as_rate_limits() {
        assert!(is_rate_limit_status(429));
        assert!(is_rate_limit_status(529));
        assert!(!is_rate_limit_status(500));
        assert!(!is_rate_limit_status(402));
    }
}
