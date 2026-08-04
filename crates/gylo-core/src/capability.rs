use std::fmt;

/// What a backend can do.
///
/// Declared as data rather than encoded in trait bounds, so a mismatch
/// produces a message naming the feature and the backend instead of a compile
/// error inside a generic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Capabilities {
    /// The name reported when something is refused.
    pub backend: &'static str,
    /// A job accepted is a job kept, even if the process dies immediately
    /// afterwards. False for any store that acknowledges before it persists.
    pub durable_acknowledgement: bool,
    /// The insert can join a transaction the caller already has open.
    pub transactional_enqueue: bool,
    pub priorities: bool,
    pub delayed_jobs: bool,
    pub unique_jobs: bool,
    pub keyed_concurrency: bool,
    pub workflows: bool,
    pub durable_steps: bool,
    pub cron: bool,
    pub results: bool,
}

/// A feature a worker was configured to use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Feature {
    DurableAcknowledgement,
    TransactionalEnqueue,
    Priorities,
    DelayedJobs,
    UniqueJobs,
    KeyedConcurrency,
    Workflows,
    DurableSteps,
    Cron,
    Results,
}

impl Feature {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::DurableAcknowledgement => "durable acknowledgement",
            Self::TransactionalEnqueue => "transactional enqueue",
            Self::Priorities => "priorities",
            Self::DelayedJobs => "delayed jobs",
            Self::UniqueJobs => "unique jobs",
            Self::KeyedConcurrency => "keyed concurrency",
            Self::Workflows => "workflows",
            Self::DurableSteps => "durable steps",
            Self::Cron => "cron",
            Self::Results => "results",
        }
    }
}

impl fmt::Display for Feature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A backend was asked for something it cannot do.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
#[error("the {backend} backend does not support {feature}")]
pub struct Unsupported {
    pub backend: &'static str,
    pub feature: Feature,
}

impl Capabilities {
    pub const fn supports(&self, feature: Feature) -> bool {
        match feature {
            Feature::DurableAcknowledgement => self.durable_acknowledgement,
            Feature::TransactionalEnqueue => self.transactional_enqueue,
            Feature::Priorities => self.priorities,
            Feature::DelayedJobs => self.delayed_jobs,
            Feature::UniqueJobs => self.unique_jobs,
            Feature::KeyedConcurrency => self.keyed_concurrency,
            Feature::Workflows => self.workflows,
            Feature::DurableSteps => self.durable_steps,
            Feature::Cron => self.cron,
            Feature::Results => self.results,
        }
    }

    /// Rejects the first requested feature this backend cannot give.
    ///
    /// The point is that there is no third answer. A feature is supported or
    /// the worker refuses to start; it is never approximated.
    pub fn require(&self, wanted: &[Feature]) -> Result<(), Unsupported> {
        for feature in wanted {
            if !self.supports(*feature) {
                return Err(Unsupported {
                    backend: self.backend,
                    feature: *feature,
                });
            }
        }
        Ok(())
    }

    /// Everything this backend cannot do, for reporting at startup.
    pub fn gaps(&self) -> Vec<Feature> {
        [
            Feature::DurableAcknowledgement,
            Feature::TransactionalEnqueue,
            Feature::Priorities,
            Feature::DelayedJobs,
            Feature::UniqueJobs,
            Feature::KeyedConcurrency,
            Feature::Workflows,
            Feature::DurableSteps,
            Feature::Cron,
            Feature::Results,
        ]
        .into_iter()
        .filter(|feature| !self.supports(*feature))
        .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const EVERYTHING: Capabilities = Capabilities {
        backend: "postgres",
        durable_acknowledgement: true,
        transactional_enqueue: true,
        priorities: true,
        delayed_jobs: true,
        unique_jobs: true,
        keyed_concurrency: true,
        workflows: true,
        durable_steps: true,
        cron: true,
        results: true,
    };

    const LOSSY: Capabilities = Capabilities {
        backend: "redis",
        durable_acknowledgement: false,
        transactional_enqueue: false,
        workflows: false,
        durable_steps: false,
        keyed_concurrency: false,
        ..EVERYTHING
    };

    #[test]
    fn a_full_backend_accepts_everything() {
        assert!(EVERYTHING.require(&EVERYTHING.gaps()).is_ok());
        assert!(EVERYTHING.gaps().is_empty());
    }

    #[test]
    fn a_missing_feature_names_itself_and_the_backend() {
        let refused = LOSSY.require(&[Feature::Workflows]).unwrap_err();

        assert_eq!(refused.backend, "redis");
        assert_eq!(refused.feature, Feature::Workflows);
        assert_eq!(
            refused.to_string(),
            "the redis backend does not support workflows"
        );
    }

    #[test]
    fn what_a_backend_can_do_is_still_accepted() {
        assert!(
            LOSSY
                .require(&[Feature::Priorities, Feature::DelayedJobs, Feature::Cron])
                .is_ok()
        );
    }

    #[test]
    fn the_first_gap_is_the_one_reported() {
        let refused = LOSSY
            .require(&[
                Feature::Priorities,
                Feature::DurableSteps,
                Feature::Workflows,
            ])
            .unwrap_err();

        assert_eq!(refused.feature, Feature::DurableSteps);
    }

    #[test]
    fn gaps_are_listable_for_a_startup_report() {
        assert_eq!(
            LOSSY.gaps(),
            vec![
                Feature::DurableAcknowledgement,
                Feature::TransactionalEnqueue,
                Feature::KeyedConcurrency,
                Feature::Workflows,
                Feature::DurableSteps,
            ]
        );
    }

    #[test]
    fn losing_accepted_jobs_is_a_capability_like_any_other() {
        assert!(
            !LOSSY.supports(Feature::DurableAcknowledgement),
            "a queue that may drop accepted work must say so in the same place \
             as every other limitation, not in a footnote"
        );
    }
}
