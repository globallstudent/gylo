use std::str::FromStr;

use chrono::{DateTime, Utc};
use chrono_tz::Tz;

/// A cron expression paired with the zone it is read in.
///
/// The zone matters for anything coarser than hourly: "0 3 * * *" in
/// Europe/London is an hour earlier in winter than in summer when measured in
/// UTC, and a schedule that drifts by an hour twice a year is a bug report.
#[derive(Debug, Clone)]
pub struct Schedule {
    cron: cron::Schedule,
    zone: Tz,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ScheduleError {
    #[error("cron expression {expression:?} is invalid: {reason}")]
    Expression { expression: String, reason: String },
    #[error("unknown timezone {0:?}")]
    Timezone(String),
    #[error("expression {0:?} has no occurrence after the given time")]
    NoOccurrence(String),
}

impl Schedule {
    /// Parses a five-field cron expression, or the six-field form with seconds.
    pub fn parse(expression: &str, timezone: &str) -> Result<Self, ScheduleError> {
        let zone =
            Tz::from_str(timezone).map_err(|_| ScheduleError::Timezone(timezone.to_owned()))?;
        let cron = cron::Schedule::from_str(&normalise(expression)).map_err(|error| {
            ScheduleError::Expression {
                expression: expression.to_owned(),
                reason: error.to_string(),
            }
        })?;
        Ok(Self { cron, zone })
    }

    /// The first occurrence strictly after `after`.
    pub fn next_after(&self, after: DateTime<Utc>) -> Result<DateTime<Utc>, ScheduleError> {
        self.cron
            .after(&after.with_timezone(&self.zone))
            .next()
            .map(|at| at.with_timezone(&Utc))
            .ok_or_else(|| ScheduleError::NoOccurrence(self.cron.to_string()))
    }
}

/// The `cron` crate expects seven fields with seconds leading and a year
/// trailing; the five-field form everyone writes has neither.
fn normalise(expression: &str) -> String {
    match expression.split_whitespace().count() {
        5 => format!("0 {expression} *"),
        6 => format!("{expression} *"),
        _ => expression.to_owned(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn at(text: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(text)
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn five_field_expressions_are_accepted() {
        let schedule = Schedule::parse("*/5 * * * *", "UTC").unwrap();
        let next = schedule.next_after(at("2026-08-04T10:02:00Z")).unwrap();
        assert_eq!(next, at("2026-08-04T10:05:00Z"));
    }

    #[test]
    fn six_field_expressions_keep_their_seconds() {
        let schedule = Schedule::parse("30 * * * * *", "UTC").unwrap();
        let next = schedule.next_after(at("2026-08-04T10:00:00Z")).unwrap();
        assert_eq!(next, at("2026-08-04T10:00:30Z"));
    }

    #[test]
    fn the_zone_decides_when_daily_means() {
        let london = Schedule::parse("0 3 * * *", "Europe/London").unwrap();

        let winter = london.next_after(at("2026-01-15T00:00:00Z")).unwrap();
        let summer = london.next_after(at("2026-07-15T00:00:00Z")).unwrap();

        assert_eq!(winter, at("2026-01-15T03:00:00Z"));
        assert_eq!(
            summer,
            at("2026-07-15T02:00:00Z"),
            "03:00 local is 02:00 UTC once British Summer Time starts"
        );
    }

    #[test]
    fn a_skipped_local_hour_does_not_lose_the_run() {
        let london = Schedule::parse("30 1 * * *", "Europe/London").unwrap();
        let next = london.next_after(at("2026-03-29T00:00:00Z")).unwrap();

        assert!(
            next > at("2026-03-29T00:00:00Z"),
            "01:30 does not exist on the spring-forward day, so it must roll to the next one"
        );
    }

    #[test]
    fn occurrences_are_strictly_after_the_given_time() {
        let schedule = Schedule::parse("0 * * * *", "UTC").unwrap();
        let exactly_due = at("2026-08-04T10:00:00Z");

        assert_eq!(
            schedule.next_after(exactly_due).unwrap(),
            at("2026-08-04T11:00:00Z"),
            "returning the same instant would fire the identical slot forever"
        );
    }

    #[test]
    fn an_invalid_expression_names_itself() {
        let error = Schedule::parse("not a cron", "UTC").unwrap_err();
        assert!(matches!(error, ScheduleError::Expression { .. }));
        assert!(error.to_string().contains("not a cron"));
    }

    #[test]
    fn an_unknown_timezone_is_rejected() {
        assert_eq!(
            Schedule::parse("* * * * *", "Mars/Olympus").unwrap_err(),
            ScheduleError::Timezone("Mars/Olympus".to_owned())
        );
    }

    #[test]
    fn zones_shift_the_utc_instant() {
        let tokyo = Schedule::parse("0 9 * * *", "Asia/Tokyo").unwrap();
        let next = tokyo.next_after(at("2026-08-03T12:00:00Z")).unwrap();
        assert_eq!(
            next,
            Utc.with_ymd_and_hms(2026, 8, 4, 0, 0, 0).unwrap(),
            "09:00 in Tokyo is midnight UTC the same calendar day"
        );
    }
}
