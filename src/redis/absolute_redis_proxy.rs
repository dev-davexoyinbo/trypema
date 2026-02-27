use crate::TrypemaError;

pub(crate) enum AbsoluteRedisProxyState {
    ReadResult {
        key: String,
        current_total_count: u64,
        window_limit: Option<u64>,
        last_rate_group_ttl: Option<u64>,
        last_rate_group_count: Option<u64>,
    },
    CommitResult {
        key: String,
        current_total_count: u64,
        window_limit: u64,
        last_rate_group_ttl: Option<u64>,
        last_rate_group_count: Option<u64>,
    },
}

pub(crate) struct AbsoluteRedisProxyReadOptions {}
pub(crate) struct AbsoluteRedisProxyCommitOptions {}

pub(crate) struct AbsoluteRedisProxy {}

impl AbsoluteRedisProxy {
    pub(crate) fn read_state(
        options: AbsoluteRedisProxyReadOptions,
    ) -> Result<AbsoluteRedisProxyState, TrypemaError> {
        todo!()
    }

    pub(crate) fn commit_state(
        options: AbsoluteRedisProxyCommitOptions,
    ) -> Result<AbsoluteRedisProxyState, TrypemaError> {
        todo!()
    }
}
