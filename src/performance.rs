//! Performance monitoring and metrics collection
//!
//! This module provides comprehensive performance tracking for all operations
//! in the canonical manager, following the ADR-002 specifications.

use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::RwLock;

/// Performance metrics collector and analyzer
pub struct PerformanceMonitor {
    metrics: Arc<RwLock<PerformanceMetrics>>,
    operation_timings: Arc<DashMap<String, Vec<Duration>>>,
    config: PerformanceConfig,
}

/// Configuration for performance monitoring
#[derive(Debug, Clone)]
pub struct PerformanceConfig {
    pub enable_metrics: bool,
    pub metrics_interval: Duration,
    pub max_samples: usize,
    pub enable_detailed_logging: bool,
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            enable_metrics: true,
            metrics_interval: Duration::from_secs(30),
            max_samples: 1000,
            enable_detailed_logging: false,
        }
    }
}

/// Comprehensive performance metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceMetrics {
    pub started_at: SystemTime,
    pub last_updated: SystemTime,

    // Index operations
    pub index_operations: IndexOperationMetrics,

    // Package operations
    pub package_operations: PackageOperationMetrics,

    // Search operations
    pub search_operations: SearchOperationMetrics,

    // System resources
    pub system_metrics: SystemMetrics,

    // Performance targets from ADR
    pub performance_targets: PerformanceTargets,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexOperationMetrics {
    pub incremental_updates_count: u64,
    pub full_rebuilds_count: u64,
    pub average_incremental_time: Duration,
    pub average_full_rebuild_time: Duration,
    pub last_rebuild_type: Option<String>,
    pub last_rebuild_duration: Option<Duration>,
    pub parallel_efficiency: f64, // 0.0 to 1.0
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageOperationMetrics {
    pub packages_installed: u64,
    pub packages_removed: u64,
    pub average_install_time: Duration,
    pub change_detection_time: Duration,
    pub checksum_computation_count: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchOperationMetrics {
    pub searches_performed: u64,
    pub average_search_time: Duration,
    pub cache_hit_rate: f64,
    pub index_size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SystemMetrics {
    pub memory_usage_bytes: u64,
    pub disk_usage_bytes: u64,
    pub compression_ratio: f64,
    pub cpu_usage_percent: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceTargets {
    pub incremental_vs_full_speedup: f64, // Target: 80-90% faster
    pub parallel_speedup: f64,            // Target: 4-8x on multi-core
    pub change_detection_time_ms: f64,    // Target: <100ms for 1000 packages
    pub compression_ratio: f64,           // Target: 50-60% size reduction
}

impl PerformanceMonitor {
    /// Create a new performance monitor
    pub fn new(config: PerformanceConfig) -> Self {
        let metrics = PerformanceMetrics {
            started_at: SystemTime::now(),
            last_updated: SystemTime::now(),
            index_operations: IndexOperationMetrics::default(),
            package_operations: PackageOperationMetrics::default(),
            search_operations: SearchOperationMetrics::default(),
            system_metrics: SystemMetrics::default(),
            performance_targets: PerformanceTargets::default(),
        };

        Self {
            metrics: Arc::new(RwLock::new(metrics)),
            operation_timings: Arc::new(DashMap::new()),
            config,
        }
    }

    /// Start tracking an operation
    pub fn start_operation(&self, operation: &str) -> OperationTracker {
        OperationTracker::new(operation.to_string(), self.operation_timings.clone())
    }

    /// Record an index operation
    pub async fn record_index_operation(
        &self,
        operation_type: IndexOperationType,
        duration: Duration,
    ) {
        if !self.config.enable_metrics {
            return;
        }

        let mut metrics = self.metrics.write().await;

        match operation_type {
            IndexOperationType::IncrementalUpdate => {
                metrics.index_operations.incremental_updates_count += 1;
                metrics.index_operations.average_incremental_time = self.update_average(
                    metrics.index_operations.average_incremental_time,
                    duration,
                    metrics.index_operations.incremental_updates_count,
                );
                metrics.index_operations.last_rebuild_type = Some("incremental".to_string());
            }
            IndexOperationType::FullRebuild => {
                metrics.index_operations.full_rebuilds_count += 1;
                metrics.index_operations.average_full_rebuild_time = self.update_average(
                    metrics.index_operations.average_full_rebuild_time,
                    duration,
                    metrics.index_operations.full_rebuilds_count,
                );
                metrics.index_operations.last_rebuild_type = Some("full".to_string());
            }
        }

        metrics.index_operations.last_rebuild_duration = Some(duration);
        metrics.last_updated = SystemTime::now();

        if self.config.enable_detailed_logging {
            tracing::info!(
                "Index operation {:?} completed in {:?}",
                operation_type,
                duration
            );
        }
    }

    /// Record a package operation
    pub async fn record_package_operation(
        &self,
        operation_type: PackageOperationType,
        duration: Duration,
    ) {
        if !self.config.enable_metrics {
            return;
        }

        let mut metrics = self.metrics.write().await;

        match operation_type {
            PackageOperationType::Install => {
                metrics.package_operations.packages_installed += 1;
                metrics.package_operations.average_install_time = self.update_average(
                    metrics.package_operations.average_install_time,
                    duration,
                    metrics.package_operations.packages_installed,
                );
            }
            PackageOperationType::Remove => {
                metrics.package_operations.packages_removed += 1;
            }
            PackageOperationType::ChangeDetection => {
                metrics.package_operations.change_detection_time = duration;
            }
            PackageOperationType::ChecksumComputation => {
                metrics.package_operations.checksum_computation_count += 1;
            }
        }

        metrics.last_updated = SystemTime::now();
    }

    /// Record a search operation
    pub async fn record_search_operation(&self, duration: Duration, cache_hit: bool) {
        if !self.config.enable_metrics {
            return;
        }

        let mut metrics = self.metrics.write().await;

        metrics.search_operations.searches_performed += 1;
        metrics.search_operations.average_search_time = self.update_average(
            metrics.search_operations.average_search_time,
            duration,
            metrics.search_operations.searches_performed,
        );

        // Update cache hit rate
        let current_hit_rate = metrics.search_operations.cache_hit_rate;
        let total_searches = metrics.search_operations.searches_performed as f64;

        if cache_hit {
            metrics.search_operations.cache_hit_rate =
                (current_hit_rate * (total_searches - 1.0) + 1.0) / total_searches;
        } else {
            metrics.search_operations.cache_hit_rate =
                (current_hit_rate * (total_searches - 1.0)) / total_searches;
        }

        metrics.last_updated = SystemTime::now();
    }

    /// Update system metrics
    pub async fn update_system_metrics(&self, system_metrics: SystemMetrics) {
        if !self.config.enable_metrics {
            return;
        }

        let mut metrics = self.metrics.write().await;
        metrics.system_metrics = system_metrics;
        metrics.last_updated = SystemTime::now();
    }

    /// Get current performance metrics
    pub async fn get_metrics(&self) -> PerformanceMetrics {
        self.metrics.read().await.clone()
    }

    /// Analyze performance and compare against targets
    pub async fn analyze_performance(&self) -> PerformanceAnalysis {
        let metrics = self.metrics.read().await;

        let incremental_speedup = if metrics
            .index_operations
            .average_full_rebuild_time
            .as_millis()
            > 0
        {
            metrics
                .index_operations
                .average_full_rebuild_time
                .as_secs_f64()
                / metrics
                    .index_operations
                    .average_incremental_time
                    .as_secs_f64()
        } else {
            0.0
        };

        let meets_incremental_target = incremental_speedup >= 5.0; // 80% faster = 5x speedup
        let meets_change_detection_target =
            metrics.package_operations.change_detection_time.as_millis() < 100;
        let meets_compression_target = metrics.system_metrics.compression_ratio >= 0.5;

        PerformanceAnalysis {
            overall_health: if meets_incremental_target
                && meets_change_detection_target
                && meets_compression_target
            {
                PerformanceHealth::Excellent
            } else if meets_incremental_target || meets_change_detection_target {
                PerformanceHealth::Good
            } else {
                PerformanceHealth::NeedsImprovement
            },
            incremental_speedup_actual: incremental_speedup,
            incremental_speedup_target: 5.0,
            change_detection_time_ms: metrics.package_operations.change_detection_time.as_millis()
                as f64,
            compression_ratio_actual: metrics.system_metrics.compression_ratio,
            compression_ratio_target: 0.55,
            recommendations: self
                .generate_recommendations(&metrics, incremental_speedup)
                .await,
        }
    }

    /// Generate performance recommendations
    async fn generate_recommendations(
        &self,
        metrics: &PerformanceMetrics,
        incremental_speedup: f64,
    ) -> Vec<String> {
        let mut recommendations = Vec::new();

        if incremental_speedup < 5.0 {
            recommendations.push(
                "Consider increasing parallel workers for better incremental update performance"
                    .to_string(),
            );
        }

        if metrics.package_operations.change_detection_time.as_millis() > 100 {
            recommendations.push(
                "Change detection is slower than target - consider optimizing checksum computation"
                    .to_string(),
            );
        }

        if metrics.system_metrics.compression_ratio < 0.5 {
            recommendations.push(
                "Compression ratio below target - consider adjusting compression level".to_string(),
            );
        }

        if metrics.search_operations.cache_hit_rate < 0.8 {
            recommendations
                .push("Search cache hit rate is low - consider increasing cache size".to_string());
        }

        if recommendations.is_empty() {
            recommendations.push("Performance is meeting all targets".to_string());
        }

        recommendations
    }

    // Helper methods

    fn update_average(&self, current_avg: Duration, new_value: Duration, count: u64) -> Duration {
        if count <= 1 {
            new_value
        } else {
            let total_ms =
                current_avg.as_millis() as u64 * (count - 1) + new_value.as_millis() as u64;
            Duration::from_millis(total_ms / count)
        }
    }
}

/// Operation tracker for measuring individual operations
pub struct OperationTracker {
    operation: String,
    start_time: Instant,
    timings: Arc<DashMap<String, Vec<Duration>>>,
}

impl OperationTracker {
    fn new(operation: String, timings: Arc<DashMap<String, Vec<Duration>>>) -> Self {
        Self {
            operation,
            start_time: Instant::now(),
            timings,
        }
    }

    /// Finish tracking and record the duration
    pub fn finish(self) -> Duration {
        let duration = self.start_time.elapsed();

        // Store timing for analysis
        self.timings
            .entry(self.operation)
            .or_default()
            .push(duration);

        duration
    }
}

/// Types of index operations for metrics tracking
#[derive(Debug, Clone)]
pub enum IndexOperationType {
    IncrementalUpdate,
    FullRebuild,
}

/// Types of package operations for metrics tracking
#[derive(Debug, Clone)]
pub enum PackageOperationType {
    Install,
    Remove,
    ChangeDetection,
    ChecksumComputation,
}

/// Performance analysis results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceAnalysis {
    pub overall_health: PerformanceHealth,
    pub incremental_speedup_actual: f64,
    pub incremental_speedup_target: f64,
    pub change_detection_time_ms: f64,
    pub compression_ratio_actual: f64,
    pub compression_ratio_target: f64,
    pub recommendations: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PerformanceHealth {
    Excellent,
    Good,
    NeedsImprovement,
}

// Default implementations

impl Default for IndexOperationMetrics {
    fn default() -> Self {
        Self {
            incremental_updates_count: 0,
            full_rebuilds_count: 0,
            average_incremental_time: Duration::from_secs(0),
            average_full_rebuild_time: Duration::from_secs(0),
            last_rebuild_type: None,
            last_rebuild_duration: None,
            parallel_efficiency: 0.0,
        }
    }
}

impl Default for PackageOperationMetrics {
    fn default() -> Self {
        Self {
            packages_installed: 0,
            packages_removed: 0,
            average_install_time: Duration::from_secs(0),
            change_detection_time: Duration::from_secs(0),
            checksum_computation_count: 0,
        }
    }
}

impl Default for SearchOperationMetrics {
    fn default() -> Self {
        Self {
            searches_performed: 0,
            average_search_time: Duration::from_secs(0),
            cache_hit_rate: 0.0,
            index_size_bytes: 0,
        }
    }
}

impl Default for SystemMetrics {
    fn default() -> Self {
        Self {
            memory_usage_bytes: 0,
            disk_usage_bytes: 0,
            compression_ratio: 0.0,
            cpu_usage_percent: 0.0,
        }
    }
}

impl Default for PerformanceTargets {
    fn default() -> Self {
        Self {
            incremental_vs_full_speedup: 5.0, // 80% faster = 5x speedup
            parallel_speedup: 6.0,            // 6x average for 4-8x range
            change_detection_time_ms: 100.0,  // <100ms target
            compression_ratio: 0.55,          // 55% average for 50-60% range
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::{Duration, sleep};

    #[tokio::test]
    async fn test_performance_monitor() {
        let monitor = PerformanceMonitor::new(PerformanceConfig::default());

        // Test operation tracking
        let tracker = monitor.start_operation("test_operation");
        sleep(Duration::from_millis(10)).await;
        let duration = tracker.finish();

        assert!(duration.as_millis() >= 10);

        // Test metrics recording
        monitor
            .record_index_operation(IndexOperationType::IncrementalUpdate, duration)
            .await;
        monitor
            .record_package_operation(PackageOperationType::Install, duration)
            .await;
        monitor.record_search_operation(duration, true).await;

        let metrics = monitor.get_metrics().await;
        assert_eq!(metrics.index_operations.incremental_updates_count, 1);
        assert_eq!(metrics.package_operations.packages_installed, 1);
        assert_eq!(metrics.search_operations.searches_performed, 1);
    }

    #[tokio::test]
    async fn test_performance_analysis() {
        let monitor = PerformanceMonitor::new(PerformanceConfig::default());

        // Record some operations to generate metrics
        monitor
            .record_index_operation(IndexOperationType::FullRebuild, Duration::from_secs(10))
            .await;

        monitor
            .record_index_operation(
                IndexOperationType::IncrementalUpdate,
                Duration::from_secs(1),
            )
            .await;

        let analysis = monitor.analyze_performance().await;
        assert!(analysis.incremental_speedup_actual > 0.0);
        assert!(!analysis.recommendations.is_empty());
    }
}
