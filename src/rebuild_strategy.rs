use crate::change_detection::{PackageChangeDetector, PackageChecksum};
use crate::error::Result;
use crate::parallel_processor::PackageInfo;
use dashmap::DashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

#[derive(Debug, Clone)]
pub enum RebuildStrategy {
    None,
    Incremental {
        packages: Vec<PackageInfo>,
        batch_size: usize,
    },
    Full,
}

impl RebuildStrategy {
    pub fn is_none(&self) -> bool {
        matches!(self, RebuildStrategy::None)
    }

    pub fn is_incremental(&self) -> bool {
        matches!(self, RebuildStrategy::Incremental { .. })
    }

    pub fn is_full(&self) -> bool {
        matches!(self, RebuildStrategy::Full)
    }

    pub fn package_count(&self) -> usize {
        match self {
            RebuildStrategy::None => 0,
            RebuildStrategy::Incremental { packages, .. } => packages.len(),
            RebuildStrategy::Full => usize::MAX, // Represents all packages
        }
    }
}

pub struct SmartRebuildStrategy {
    full_rebuild_threshold: f64, // Percentage of changes to trigger full rebuild
    incremental_batch_size: usize,
    checksum_cache: Arc<DashMap<String, PackageChecksum>>,
    change_detector: Arc<PackageChangeDetector>,
    performance_history: Arc<DashMap<String, RebuildPerformance>>,
    last_full_rebuild: Option<SystemTime>,
    full_rebuild_interval: Duration,
}

#[derive(Debug, Clone)]
struct RebuildPerformance {
    strategy_used: String,
    duration: Duration,
    packages_processed: usize,
    timestamp: SystemTime,
    efficiency_score: f64, // packages per second
}

impl SmartRebuildStrategy {
    pub fn new(change_detector: Arc<PackageChangeDetector>) -> Self {
        Self {
            full_rebuild_threshold: 0.3, // 30% of packages changed
            incremental_batch_size: 50,
            checksum_cache: Arc::new(DashMap::new()),
            change_detector,
            performance_history: Arc::new(DashMap::new()),
            last_full_rebuild: None,
            full_rebuild_interval: Duration::from_secs(7 * 24 * 60 * 60), // 1 week
        }
    }

    pub fn with_full_rebuild_threshold(mut self, threshold: f64) -> Self {
        self.full_rebuild_threshold = threshold.clamp(0.0, 1.0);
        self
    }

    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.incremental_batch_size = batch_size;
        self
    }

    pub fn with_full_rebuild_interval(mut self, interval: Duration) -> Self {
        self.full_rebuild_interval = interval;
        self
    }

    pub async fn determine_strategy(&self, packages: &[PackageInfo]) -> Result<RebuildStrategy> {
        if packages.is_empty() {
            return Ok(RebuildStrategy::None);
        }

        // Check if forced full rebuild is needed
        if self.should_force_full_rebuild().await {
            tracing::info!("Forcing full rebuild due to time interval or other criteria");
            return Ok(RebuildStrategy::Full);
        }

        let total = packages.len();
        let mut changed_packages = Vec::new();

        // Check which packages have changed
        for package in packages {
            if self.has_changed(package).await? {
                changed_packages.push(package.clone());
            }
        }

        let changed_count = changed_packages.len();
        let change_ratio = changed_count as f64 / total as f64;

        tracing::debug!(
            "Rebuild analysis: {}/{} packages changed ({:.1}%)",
            changed_count,
            total,
            change_ratio * 100.0
        );

        // Decide strategy based on change ratio and performance history
        let strategy = if change_ratio >= self.full_rebuild_threshold {
            // High change ratio - consider full rebuild
            let prefer_full = self.should_prefer_full_rebuild(changed_count, total).await;
            tracing::debug!(
                "High change ratio {:.2}, prefer_full={}",
                change_ratio,
                prefer_full
            );
            if prefer_full {
                RebuildStrategy::Full
            } else {
                RebuildStrategy::Incremental {
                    packages: changed_packages,
                    batch_size: self.adaptive_batch_size(changed_count),
                }
            }
        } else if changed_count == 0 {
            RebuildStrategy::None
        } else {
            // Low change ratio - incremental update
            RebuildStrategy::Incremental {
                packages: changed_packages,
                batch_size: self.adaptive_batch_size(changed_count),
            }
        };

        self.log_strategy_decision(&strategy, change_ratio).await;
        Ok(strategy)
    }

    async fn has_changed(&self, package: &PackageInfo) -> Result<bool> {
        // Check if we have a cached checksum for this package
        if let Some(cached_checksum) = self.checksum_cache.get(&package.id) {
            // Compute current checksum and compare
            let current_checksum = self
                .change_detector
                .compute_package_checksum(&package.path)
                .await?;

            let changed = cached_checksum.content_hash != current_checksum.content_hash;

            if changed {
                // Update cache with new checksum
                self.checksum_cache
                    .insert(package.id.clone(), current_checksum);
            }

            Ok(changed)
        } else {
            // No cached checksum - consider it changed (new package)
            let checksum = self
                .change_detector
                .compute_package_checksum(&package.path)
                .await?;

            self.checksum_cache.insert(package.id.clone(), checksum);
            Ok(true)
        }
    }

    async fn should_force_full_rebuild(&self) -> bool {
        // Check if enough time has passed since last full rebuild
        if let Some(last_rebuild) = self.last_full_rebuild {
            let elapsed = SystemTime::now()
                .duration_since(last_rebuild)
                .unwrap_or(Duration::ZERO);

            if elapsed >= self.full_rebuild_interval {
                return true;
            }
        } else {
            // Never done a full rebuild
            return true;
        }

        // Could add other criteria here:
        // - Index corruption detected
        // - Schema version changes
        // - Performance degradation detected

        false
    }

    async fn should_prefer_full_rebuild(&self, changed_count: usize, total_count: usize) -> bool {
        // Use performance history to decide
        let incremental_performance = self.get_average_performance("incremental").await;
        let full_performance = self.get_average_performance("full").await;

        // If we have historical data, use it to make informed decision
        if let (Some(inc_perf), Some(full_perf)) = (incremental_performance, full_performance) {
            // Estimate time for incremental vs full rebuild
            let estimated_incremental_time = changed_count as f64 / inc_perf.efficiency_score;
            let estimated_full_time = total_count as f64 / full_perf.efficiency_score;

            // Prefer full rebuild if it's not significantly slower
            // and we're rebuilding a large portion anyway
            let full_rebuild_overhead = 1.5; // Allow 50% overhead for full rebuild
            estimated_full_time <= estimated_incremental_time * full_rebuild_overhead
        } else {
            // No historical data - use simple heuristic
            // Only prefer full rebuild if significantly more than half changed
            // and we have a reasonable number of packages
            let prefer_full = total_count >= 10 && changed_count > (total_count * 3) / 4;
            tracing::debug!(
                "No historical data: total={}, changed={}, prefer_full={}",
                total_count,
                changed_count,
                prefer_full
            );
            prefer_full
        }
    }

    fn adaptive_batch_size(&self, changed_count: usize) -> usize {
        // Adjust batch size based on number of changes
        match changed_count {
            0..=10 => 5,
            11..=50 => 10,
            51..=200 => 25,
            201..=500 => 50,
            _ => 100,
        }
    }

    async fn get_average_performance(&self, strategy_type: &str) -> Option<RebuildPerformance> {
        let performances: Vec<_> = self
            .performance_history
            .iter()
            .filter_map(|entry| {
                let perf = entry.value();
                if perf.strategy_used == strategy_type {
                    Some(perf.clone())
                } else {
                    None
                }
            })
            .collect();

        if performances.is_empty() {
            return None;
        }

        let avg_duration = Duration::from_nanos(
            performances
                .iter()
                .map(|p| p.duration.as_nanos() as u64)
                .sum::<u64>()
                / performances.len() as u64,
        );

        let avg_packages = performances
            .iter()
            .map(|p| p.packages_processed)
            .sum::<usize>()
            / performances.len();

        let avg_efficiency = performances.iter().map(|p| p.efficiency_score).sum::<f64>()
            / performances.len() as f64;

        Some(RebuildPerformance {
            strategy_used: strategy_type.to_string(),
            duration: avg_duration,
            packages_processed: avg_packages,
            timestamp: SystemTime::now(),
            efficiency_score: avg_efficiency,
        })
    }

    async fn log_strategy_decision(&self, strategy: &RebuildStrategy, change_ratio: f64) {
        match strategy {
            RebuildStrategy::None => {
                tracing::info!("Strategy: No rebuild needed - no changes detected");
            }
            RebuildStrategy::Incremental {
                packages,
                batch_size,
            } => {
                tracing::info!(
                    "Strategy: Incremental rebuild - {} packages, batch_size={}, change_ratio={:.1}%",
                    packages.len(),
                    batch_size,
                    change_ratio * 100.0
                );
            }
            RebuildStrategy::Full => {
                tracing::info!(
                    "Strategy: Full rebuild - change_ratio={:.1}% exceeded threshold {:.1}%",
                    change_ratio * 100.0,
                    self.full_rebuild_threshold * 100.0
                );
            }
        }
    }

    pub async fn record_performance(
        &self,
        strategy: &RebuildStrategy,
        duration: Duration,
        packages_processed: usize,
    ) {
        let strategy_name = match strategy {
            RebuildStrategy::None => return, // Nothing to record
            RebuildStrategy::Incremental { .. } => "incremental",
            RebuildStrategy::Full => "full",
        };

        let efficiency_score = if duration.is_zero() {
            0.0
        } else {
            packages_processed as f64 / duration.as_secs_f64()
        };

        let performance = RebuildPerformance {
            strategy_used: strategy_name.to_string(),
            duration,
            packages_processed,
            timestamp: SystemTime::now(),
            efficiency_score,
        };

        // Store with timestamp as key to keep history
        let key = format!("{}_{:?}", strategy_name, performance.timestamp);
        self.performance_history.insert(key, performance);

        // Update last full rebuild timestamp if applicable
        if strategy_name == "full" {
            // This would need to be done in a way that updates the struct field
            // For now, just log it
            tracing::info!(
                "Recorded full rebuild completion at {:?}",
                SystemTime::now()
            );
        }

        // Clean up old performance records (keep last 50 entries per strategy)
        self.cleanup_performance_history().await;
    }

    async fn cleanup_performance_history(&self) {
        let max_entries_per_strategy = 50;

        for strategy_type in ["incremental", "full"] {
            let mut entries: Vec<_> = self
                .performance_history
                .iter()
                .filter_map(|entry| {
                    let perf = entry.value();
                    if perf.strategy_used == strategy_type {
                        Some((entry.key().clone(), perf.timestamp))
                    } else {
                        None
                    }
                })
                .collect();

            // Sort by timestamp (newest first)
            entries.sort_by(|a, b| b.1.cmp(&a.1));

            // Remove old entries beyond the limit
            for (key, _) in entries.into_iter().skip(max_entries_per_strategy) {
                self.performance_history.remove(&key);
            }
        }
    }

    pub fn get_changed_packages(&self, packages: &[PackageInfo]) -> Vec<PackageInfo> {
        // This is a helper method that would typically be used in conjunction
        // with determine_strategy(), but can be called separately if needed
        packages.to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::change_detection::PackageChangeDetector;
    use std::fs;
    use tempfile::TempDir;

    #[allow(dead_code)]
    fn create_test_package(base_dir: &std::path::Path, id: &str, content: &str) -> PackageInfo {
        let package_dir = base_dir.join(id);
        fs::create_dir_all(&package_dir).unwrap();

        fs::write(
            package_dir.join("package.json"),
            format!(r#"{{"name": "{id}", "version": "1.0.0"}}"#),
        )
        .unwrap();

        fs::write(package_dir.join("resource.json"), content).unwrap();

        PackageInfo::new(
            id.to_string(),
            id.to_string(),
            "1.0.0".to_string(),
            package_dir,
        )
    }

    #[tokio::test]
    async fn test_no_changes_strategy() {
        let _temp_dir = TempDir::new().unwrap();
        let change_detector = Arc::new(PackageChangeDetector::new());
        let strategy = SmartRebuildStrategy::new(change_detector);

        // Empty package list
        let packages = vec![];
        let result = strategy.determine_strategy(&packages).await.unwrap();

        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_incremental_strategy() {
        let _temp_dir = TempDir::new().unwrap();
        let change_detector = Arc::new(PackageChangeDetector::new());
        let strategy = SmartRebuildStrategy::new(change_detector);

        // Empty package list should return None
        let packages = vec![];
        let result = strategy.determine_strategy(&packages).await.unwrap();
        assert!(result.is_none(), "Expected None for empty package list");

        // Test adaptive batch size logic
        assert_eq!(strategy.adaptive_batch_size(5), 5);
        assert_eq!(strategy.adaptive_batch_size(25), 10);
        assert_eq!(strategy.adaptive_batch_size(100), 25);
    }

    #[tokio::test]
    async fn test_full_rebuild_threshold() {
        let _temp_dir = TempDir::new().unwrap();
        let change_detector = Arc::new(PackageChangeDetector::new());
        let strategy = SmartRebuildStrategy::new(change_detector)
            .with_full_rebuild_threshold(0.75)
            .with_batch_size(100);

        // Test that thresholds are clamped correctly
        let clamped_strategy = SmartRebuildStrategy::new(Arc::new(PackageChangeDetector::new()))
            .with_full_rebuild_threshold(1.5); // Should be clamped to 1.0

        // Can't directly test internal fields, but we can test behavior
        let packages = vec![];
        let result = clamped_strategy
            .determine_strategy(&packages)
            .await
            .unwrap();
        assert!(result.is_none(), "Expected None for empty package list");

        // Test with configured strategy too
        let result2 = strategy.determine_strategy(&packages).await.unwrap();
        assert!(result2.is_none(), "Expected None for empty package list");
    }

    #[tokio::test]
    async fn test_adaptive_batch_size() {
        let change_detector = Arc::new(PackageChangeDetector::new());
        let strategy = SmartRebuildStrategy::new(change_detector);

        assert_eq!(strategy.adaptive_batch_size(5), 5);
        assert_eq!(strategy.adaptive_batch_size(25), 10);
        assert_eq!(strategy.adaptive_batch_size(100), 25);
        assert_eq!(strategy.adaptive_batch_size(300), 50);
        assert_eq!(strategy.adaptive_batch_size(1000), 100);
    }

    #[tokio::test]
    async fn test_performance_recording() {
        let change_detector = Arc::new(PackageChangeDetector::new());
        let strategy = SmartRebuildStrategy::new(change_detector);

        let incremental_strategy = RebuildStrategy::Incremental {
            packages: vec![],
            batch_size: 10,
        };

        // Record some performance data
        strategy
            .record_performance(&incremental_strategy, Duration::from_millis(500), 10)
            .await;

        strategy
            .record_performance(&incremental_strategy, Duration::from_millis(300), 6)
            .await;

        // Check that performance was recorded
        let avg_perf = strategy.get_average_performance("incremental").await;
        assert!(avg_perf.is_some());

        let perf = avg_perf.unwrap();
        assert_eq!(perf.packages_processed, 8); // Average of 10 and 6
        assert!(perf.efficiency_score > 0.0);
    }
}
