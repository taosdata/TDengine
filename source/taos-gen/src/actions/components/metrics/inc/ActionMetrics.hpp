#pragma once

#include <vector>
#include <mutex>
#include <algorithm>
#include <string>

class ActionMetrics {
public:
    ActionMetrics(size_t sample_count = 10000);

    // Add a sample to the thread-local bucket
    void add_sample(double duration_ms);

    // Merge data from other thread instances
    void merge_from(const ActionMetrics& other);
    void merge_from(const std::vector<ActionMetrics>& others);
    
    // Calculate final metrics
    void calculate();
    
    // Get a summary of the statistics
    std::string get_summary() const;
    
    // Reset the statistical data
    void reset();
    
    // Get samples
    const std::vector<double>& get_samples() const;

    double get_min() const { return metrics_.min; }
    double get_max() const { return metrics_.max; }
    double get_avg() const { return metrics_.avg; }
    double get_sum() const { return metrics_.sum; }
    double get_p90() const { return metrics_.p90; }
    double get_p95() const { return metrics_.p95; }
    double get_p99() const { return metrics_.p99; }

private:
    // Samples data
    std::vector<double> samples_;
    
    // Final calculated results
    struct {
        double min = 0.0;
        double max = 0.0;
        double avg = 0.0;
        double sum = 0.0;
        double p90 = 0.0;
        double p95 = 0.0;
        double p99 = 0.0;
    } metrics_;
    
    // Calculate percentile using linear interpolation
    void calculate_percentile(double percentile, double& result);
};