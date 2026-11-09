#pragma once

#include "adaptive_io/io.hpp"

#include <list>
#include <unordered_map>

namespace aio {

struct FetchPlan {
    std::size_t bytes = 16 * kib;
    bool sequential = false;
    double continuation_probability = 0;
};

class FetchPolicy {
  public:
    explicit FetchPolicy(std::size_t maximum_bytes = mib, RefillRule rule = RefillRule::cost);
    FetchPlan observe(std::uint64_t stream, std::uint64_t offset, std::size_t length,
                      std::uint64_t image_bytes);
    void sample(std::size_t bytes, double service_us, double queue_us);
    void miss(std::uint64_t stream, std::uint64_t request_end);
    std::uint64_t frontier(std::uint64_t stream) const;
    void frontier(std::uint64_t stream, std::uint64_t end);
    double fixed_us() const;
    double bandwidth_mib_s() const;
    double queue_us() const;
    std::size_t stream_count() const;

  private:
    struct Stream {
        std::uint64_t end = 0;
        std::uint64_t prefetch_end = 0;
        double probability = 0;
        double request_bytes = block_size;
        std::size_t streak = 0;
        std::size_t next_window = 16 * kib;
        bool seen = false;
        std::list<std::uint64_t>::iterator position;
    };
    std::size_t maximum_;
    RefillRule rule_;
    std::list<std::uint64_t> recent_;
    std::unordered_map<std::uint64_t, Stream> streams_;
    double overhead_us_ = 200;
    double micros_per_kib_ = 1e6 / (256 * 1024);
    double queue_us_ = 0;
    double weight_ = 0;
    double x_ = 0;
    double y_ = 0;
    double xx_ = 0;
    double xy_ = 0;
};

} // namespace aio
