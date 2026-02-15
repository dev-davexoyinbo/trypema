use crate::{HardLimitFactor, RateGroupSizeMs, RateLimit, WindowSizeSeconds};

#[test]
fn rate_limit_try_from_validates_positive() {
    let rl = RateLimit::try_from(1f64).unwrap();
    assert_eq!(*rl, 1f64);

    assert_eq!(
        RateLimit::try_from(0f64).unwrap_err(),
        "Rate limit must be greater than 0"
    );
    assert_eq!(
        RateLimit::try_from(-1f64).unwrap_err(),
        "Rate limit must be greater than 0"
    );

    assert!(*RateLimit::max() > 0f64);
}

#[test]
fn window_size_seconds_try_from_validates_min_1() {
    let w = WindowSizeSeconds::try_from(1u64).unwrap();
    assert_eq!(*w, 1u64);

    assert_eq!(
        WindowSizeSeconds::try_from(0u64).unwrap_err(),
        "Window size must be at least 1"
    );
}

#[test]
fn rate_group_size_ms_try_from_validates_nonzero() {
    let g = RateGroupSizeMs::try_from(1u64).unwrap();
    assert_eq!(*g, 1u64);

    assert_eq!(
        RateGroupSizeMs::try_from(0u64).unwrap_err(),
        "Rate group size must be greater than 0"
    );
}

#[test]
fn hard_limit_factor_default_and_try_from_validate_positive() {
    let d = HardLimitFactor::default();
    assert_eq!(*d, 1f64);

    let h = HardLimitFactor::try_from(2f64).unwrap();
    assert_eq!(*h, 2f64);

    assert_eq!(
        HardLimitFactor::try_from(0f64).unwrap_err(),
        "Hard limit factor must be greater than 0"
    );
    assert_eq!(
        HardLimitFactor::try_from(-1f64).unwrap_err(),
        "Hard limit factor must be greater than 0"
    );
}
