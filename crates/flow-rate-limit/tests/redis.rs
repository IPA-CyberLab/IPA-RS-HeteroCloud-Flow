use std::{env, net::IpAddr, time::Duration};

use flow_rate_limit::{IpRateLimiter, RateLimitPolicy, RedisBackend};
use uuid::Uuid;

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
    let service_instance_id = Uuid::new_v4();
    let other_service_instance_id = Uuid::new_v4();

    let first = first_replica
        .check_service(service_instance_id, address, policy)
        .await
        .unwrap();
    let second = second_replica
        .check_service(service_instance_id, address, policy)
        .await
        .unwrap();
    let rejected = first_replica
        .check_service(service_instance_id, address, policy)
        .await
        .unwrap();

    assert!(first.allowed);
    assert!(second.allowed);
    assert!(!rejected.allowed);
    assert_eq!(rejected.remaining, 0);
    assert!(rejected.retry_after_seconds >= 1);
    assert!(
        second_replica
            .check_service(other_service_instance_id, address, policy)
            .await
            .unwrap()
            .allowed
    );

    let adjustable_service_id = Uuid::new_v4();
    let larger_policy = RateLimitPolicy::new(100, 10).unwrap();
    assert!(
        first_replica
            .check_service(adjustable_service_id, address, larger_policy)
            .await
            .unwrap()
            .allowed
    );
    let smaller_policy = RateLimitPolicy::new(1, 1).unwrap();
    let after_policy_change = second_replica
        .check_service(adjustable_service_id, address, smaller_policy)
        .await
        .unwrap();
    assert!(after_policy_change.allowed);
    assert_eq!(after_policy_change.limit, 1);
    assert_eq!(after_policy_change.remaining, 0);
    assert!(
        !first_replica
            .check_service(adjustable_service_id, address, smaller_policy)
            .await
            .unwrap()
            .allowed
    );

    tokio::time::sleep(Duration::from_millis(1_100)).await;
    assert!(
        second_replica
            .check_service(service_instance_id, address, policy)
            .await
            .unwrap()
            .allowed
    );
}
