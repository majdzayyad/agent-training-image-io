#include "adaptive_io/policy.hpp"

#include <algorithm>
#include <cmath>
#include <limits>
#include <stdexcept>

namespace aio {

FetchPolicy::FetchPolicy(std::size_t maximum_bytes, RefillRule rule)
    : maximum_(maximum_bytes), rule_(rule) {
    if (maximum_ < block_size || maximum_ % block_size != 0) {
        throw std::invalid_argument("policy maximum must contain whole cache blocks");
    }
}

FetchPlan FetchPolicy::observe(std::uint64_t stream, std::uint64_t offset, std::size_t length,
                               std::uint64_t image_bytes) {
    check_range(image_bytes, offset, length);
    if (length == 0) {
        return {};
    }
    if (!streams_.contains(stream)) {
        if (streams_.size() == 1024) {
            streams_.erase(recent_.back());
            recent_.pop_back();
        }
        recent_.push_front(stream);
        Stream state;
        state.position = recent_.begin();
        try {
            streams_.emplace(stream, state);
        } catch (...) {
            recent_.pop_front();
            throw;
        }
    }
    auto& state = streams_.at(stream);
    recent_.splice(recent_.begin(), recent_, state.position);
    const bool adjacent = state.seen && offset == state.end;
    state.probability = adjacent ? 0.75 * state.probability + 0.25 : 0.5 * state.probability;
    state.streak = adjacent ? state.streak + 1 : 0;
    if (!adjacent) {
        state.prefetch_end = 0;
        state.next_window = std::min(16 * kib, maximum_);
    }
    state.request_bytes = state.seen ? 0.75 * state.request_bytes + 0.25 * length : length;
    state.seen = true;
    state.end = offset + length;
    FetchPlan plan;
    plan.sequential = state.streak >= 2 && state.probability >= 0.6;
    plan.continuation_probability = state.probability;
    if (rule_ == RefillRule::grow_reset) {
        plan.bytes = state.next_window;
        return plan;
    }
    if (rule_ == RefillRule::locality_only) {
        const auto expected_run = std::max<double>(block_size, state.request_bytes) /
                                  std::max(1e-8, 1 - state.probability);
        const auto target = std::min<double>(maximum_, std::max<double>(length, expected_run));
        for (const auto candidate : {16 * kib, 64 * kib, 256 * kib, mib}) {
            plan.bytes = std::min(candidate, maximum_);
            if (plan.bytes >= target) {
                break;
            }
        }
        return plan;
    }
    const auto scored_offset = rule_ == RefillRule::alignment_agnostic ? 0 : offset;
    const auto scored_end = scored_offset + length;
    const auto scored_image =
        rule_ == RefillRule::alignment_agnostic ? image_bytes - offset : image_bytes;
    double best = std::numeric_limits<double>::infinity();
    for (const auto candidate : {16 * kib, 64 * kib, 256 * kib, mib}) {
        const auto size = std::min(candidate, maximum_);
        const auto begin = scored_offset / size * size;
        const auto last_window = (scored_end - 1) / size;
        const auto windows = last_window - scored_offset / size + 1;
        const auto last_begin = last_window * size;
        const auto end = last_begin + std::min<std::uint64_t>(size, scored_image - last_begin);
        const auto transferred = end - begin;
        const auto future_bytes = static_cast<double>(end - scored_end);
        const auto unit = std::max<double>(block_size, state.request_bytes);
        const auto steps = std::floor(future_bytes / unit);
        const auto fraction = future_bytes - steps * unit;
        const auto p = state.probability;
        const auto extra = p > 1 - 1e-8 ? future_bytes
                                        : unit * p * (1 - std::pow(p, steps)) / (1 - p) +
                                              fraction * std::pow(p, steps + 1);
        const auto cost =
            (windows * (overhead_us_ + queue_us_) + transferred / double(kib) * micros_per_kib_) /
            (length + extra);
        if (cost < best) {
            best = cost;
            plan.bytes = size;
        }
    }
    return plan;
}

void FetchPolicy::sample(std::size_t bytes, double service_us, double queue_us) {
    if (rule_ == RefillRule::fixed_cost || rule_ == RefillRule::locality_only ||
        rule_ == RefillRule::grow_reset) {
        return;
    }
    if (bytes == 0 || !std::isfinite(service_us) || service_us <= 0 || !std::isfinite(queue_us) ||
        queue_us < 0) {
        return;
    }
    constexpr double decay = 0.95;
    const auto x = bytes / double(kib);
    weight_ = decay * weight_ + 1;
    x_ = decay * x_ + x;
    y_ = decay * y_ + service_us;
    xx_ = decay * xx_ + x * x;
    xy_ = decay * xy_ + x * service_us;
    queue_us_ = 0.8 * queue_us_ + 0.2 * queue_us;
    const auto variance = xx_ - x_ * x_ / weight_;
    if (variance > 64) {
        const auto slope = (xy_ - x_ * y_ / weight_) / variance;
        const auto intercept = (y_ - slope * x_) / weight_;
        if (slope > 0 && intercept >= 0) {
            micros_per_kib_ = std::clamp(slope, 1e6 / (32768 * 1024.0), 1e6 / 1024.0);
            overhead_us_ = std::clamp(intercept, 1.0, 1e6);
        }
    }
}

void FetchPolicy::miss(std::uint64_t stream, std::uint64_t request_end) {
    if (rule_ != RefillRule::grow_reset) {
        return;
    }
    if (auto it = streams_.find(stream); it != streams_.end() && it->second.end == request_end) {
        it->second.next_window = std::min({maximum_, mib, 2 * it->second.next_window});
    }
}

std::uint64_t FetchPolicy::frontier(std::uint64_t stream) const {
    const auto it = streams_.find(stream);
    return it == streams_.end() ? 0 : it->second.prefetch_end;
}

void FetchPolicy::frontier(std::uint64_t stream, std::uint64_t end) {
    if (auto it = streams_.find(stream); it != streams_.end()) {
        it->second.prefetch_end = end;
    }
}

double FetchPolicy::fixed_us() const {
    return overhead_us_;
}

double FetchPolicy::bandwidth_mib_s() const {
    return 1e6 / (micros_per_kib_ * 1024);
}

double FetchPolicy::queue_us() const {
    return queue_us_;
}

std::size_t FetchPolicy::stream_count() const {
    return streams_.size();
}

} // namespace aio
