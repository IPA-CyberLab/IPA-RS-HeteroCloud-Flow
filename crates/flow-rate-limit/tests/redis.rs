use std::{env, net::IpAddr, time::Duration};

use flow_rate_limit::{IpRateLimiter, RateLimitPolicy, RedisBackend};

#[tokio::test]
async fn token_bucket_is_shared_across_process_local_limiter_instances() {
    let Ok(redis_url) = env::var("TEST_REDIS_URL") else {
        eprintln!("TEST_REDIS_URL is not set; skipping Redis integration test");
        return;
    };
    let policy = RateLimitPolicy::new(1, 2).unwrap();
    let first_replica = IpRateLimiter::new(RedisBackend::direct(&redis_url).unwrap(), policy);
    let second_replica = IpRateLimiter::new(RedisBackend::direct(&redis_url).unwrap(), policy);
    let address: IpAddr = "198.51.100.231".parse().unwrap();

    let first = first_replica.check(address).await.unwrap();
    let second = second_replica.check(address).await.unwrap();
    let rejected = first_replica.check(address).await.unwrap();

    assert!(first.allowed);
    assert!(second.allowed);
    assert!(!rejected.allowed);
    assert_eq!(rejected.remaining, 0);
    assert!(rejected.retry_after_seconds >= 1);

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert!(second_replica.check(address).await.unwrap().allowed);
}
