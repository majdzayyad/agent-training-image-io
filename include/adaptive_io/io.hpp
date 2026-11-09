#pragma once

#include <array>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace aio {

using Bytes = std::vector<std::byte>;
using Clock = std::chrono::steady_clock;
inline constexpr std::size_t block_size = 4096;
inline constexpr std::size_t kib = 1024;
inline constexpr std::size_t mib = 1024 * kib;

struct FetchResult {
    Bytes data;
    std::chrono::nanoseconds backend_queue{};
};

class RemoteBlockStore {
  public:
    virtual ~RemoteBlockStore() = default;
    virtual std::uint64_t size() const = 0;
    virtual FetchResult read(std::uint64_t offset, std::size_t length) = 0;
};

class FileStore final : public RemoteBlockStore {
  public:
    explicit FileStore(const std::string& path);
    ~FileStore() override;
    FileStore(const FileStore&) = delete;
    FileStore& operator=(const FileStore&) = delete;
    std::uint64_t size() const override;
    FetchResult read(std::uint64_t offset, std::size_t length) override;

  private:
    int fd_ = -1;
    std::uint64_t size_ = 0;
};

class HttpStore final : public RemoteBlockStore {
  public:
    explicit HttpStore(std::string url);
    std::uint64_t size() const override;
    FetchResult read(std::uint64_t offset, std::size_t length) override;

  private:
    std::string url_;
    std::string etag_;
    std::uint64_t size_ = 0;
};

// Synthetic per-request service latency plus one shared, serialized byte budget.
class ControlledStore final : public RemoteBlockStore {
  public:
    ControlledStore(std::shared_ptr<RemoteBlockStore> source, double latency_us,
                    double bandwidth_mib_s);
    ~ControlledStore() override;
    std::uint64_t size() const override;
    FetchResult read(std::uint64_t offset, std::size_t length) override;

  private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

enum class Policy { fixed, adaptive_v1, adaptive_v2, adaptive_v3, adaptive_v4 };

enum class RefillRule { cost, grow_reset, fixed_cost, locality_only, alignment_agnostic };
enum class QueueFeedback { decomposed, observable_only };

struct Config {
    std::size_t cache_bytes = 64 * mib;
    std::size_t fetch_bytes = 256 * kib;
    std::size_t workers = 4;
    std::size_t max_inflight_bytes = 8 * mib;
    Policy policy = Policy::fixed;
    RefillRule refill_rule = RefillRule::cost;
    QueueFeedback queue_feedback = QueueFeedback::decomposed;
};

struct Stats {
    std::uint64_t foreground_reads = 0;
    std::uint64_t logical_bytes = 0;
    std::uint64_t hit_blocks = 0;
    std::uint64_t miss_blocks = 0;
    std::uint64_t shared_misses = 0;
    std::uint64_t remote_requests = 0;
    std::uint64_t remote_bytes = 0;
    std::uint64_t failed_fetches = 0;
    std::uint64_t cache_bytes = 0;
    std::uint64_t peak_cache_bytes = 0;
    std::uint64_t peak_inflight_bytes = 0;
    double queue_us = 0;
    double service_us = 0;
    std::uint64_t prefetch_bytes = 0;
    std::uint64_t useful_prefetch_bytes = 0;
    std::uint64_t wasted_prefetch_bytes = 0;
    std::uint64_t prefetch_admissions = 0;
    std::uint64_t prefetch_rejections = 0;
    std::uint64_t promoted_prefetches = 0;
    std::uint64_t unused_prefetch_cache_bytes = 0;
    std::array<std::uint64_t, 4> fetch_size_counts{};
    double estimated_fixed_us = 0;
    double estimated_bandwidth_mib_s = 0;
    double estimated_queue_us = 0;
    std::uint64_t cache_admission_failures = 0;
};

class ImageReader {
  public:
    ImageReader(std::shared_ptr<RemoteBlockStore> source, Config config = {});
    ~ImageReader();
    ImageReader(const ImageReader&) = delete;
    ImageReader& operator=(const ImageReader&) = delete;
    Bytes read(std::uint64_t offset, std::size_t length, std::uint64_t stream_id = 0);
    void drain();
    Stats stats() const;

  private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

void check_range(std::uint64_t size, std::uint64_t offset, std::size_t length);

} // namespace aio
