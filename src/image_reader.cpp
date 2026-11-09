#include "adaptive_io/io.hpp"
#include "adaptive_io/policy.hpp"

#include <algorithm>
#include <condition_variable>
#include <cstring>
#include <deque>
#include <future>
#include <list>
#include <mutex>
#include <stdexcept>
#include <thread>
#include <unordered_map>
#include <utility>

namespace aio {

struct ImageReader::Impl {
    struct Data {
        Bytes bytes;
        bool speculative = false;
        bool used = false;
        bool resident = false;
        bool counted_wasted = false;
    };
    using Block = std::shared_ptr<Data>;
    using Promise = std::shared_ptr<std::promise<Block>>;
    struct Entry {
        Block data;
        std::list<std::uint64_t>::iterator position;
    };
    struct Job {
        std::uint64_t first;
        std::vector<Promise> promises;
        Clock::time_point submitted;
        bool speculative = false;
        bool background = false;
        bool running = false;
        bool dispatched_background = false;
    };
    struct Flight {
        std::shared_future<Block> future;
        std::weak_ptr<Job> job;
    };

    std::shared_ptr<RemoteBlockStore> source;
    Config config;
    FetchPolicy policy;
    mutable std::mutex mutex;
    std::condition_variable changed;
    std::unordered_map<std::uint64_t, Entry> cache;
    std::list<std::uint64_t> lru;
    std::unordered_map<std::uint64_t, Flight> flights;
    std::deque<std::shared_ptr<Job>> demand_jobs;
    std::deque<std::shared_ptr<Job>> prefetch_jobs;
    std::vector<std::thread> workers;
    bool stopping = false;
    std::size_t active = 0;
    std::size_t active_prefetch = 0;
    std::size_t reserved_prefetch_blocks = 0;
    std::size_t low_usefulness_attempts = 0;
    Stats counters;

    Impl(std::shared_ptr<RemoteBlockStore> src, Config cfg)
        : source(std::move(src)), config(cfg),
          policy(std::min({cfg.cache_bytes, cfg.max_inflight_bytes, mib}), cfg.refill_rule) {
        if (!source || config.workers == 0 || config.workers > 256 ||
            config.cache_bytes < block_size || config.cache_bytes % block_size != 0 ||
            config.fetch_bytes < block_size || config.fetch_bytes % block_size != 0 ||
            config.fetch_bytes > mib || config.max_inflight_bytes < block_size ||
            config.max_inflight_bytes % block_size != 0) {
            throw std::invalid_argument("invalid cache, fetch, worker, or in-flight budget");
        }
        config.fetch_bytes =
            std::min({config.fetch_bytes, config.cache_bytes, config.max_inflight_bytes});
        try {
            for (std::size_t i = 0; i < config.workers; ++i) {
                workers.emplace_back([this] { worker(); });
            }
        } catch (...) {
            stop();
            throw;
        }
    }

    ~Impl() {
        stop();
    }

    void stop() {
        {
            std::lock_guard lock(mutex);
            stopping = true;
        }
        changed.notify_all();
        for (auto& worker_thread : workers) {
            worker_thread.join();
        }
    }

    bool prefetch_enabled() const {
        return config.policy == Policy::adaptive_v2 || config.policy == Policy::adaptive_v3 ||
               config.policy == Policy::adaptive_v4;
    }

    bool congestion_control() const {
        return config.policy == Policy::adaptive_v3 || config.policy == Policy::adaptive_v4;
    }

    std::size_t prefetch_limit() const {
        if (!congestion_control()) {
            return config.workers;
        }
        const auto maximum = config.workers - 1;
        if (!demand_jobs.empty() || policy.queue_us() > 2 * policy.fixed_us()) {
            return std::min<std::size_t>(1, maximum);
        }
        return maximum;
    }

    void insert(std::uint64_t id, Block data) {
        while (counters.cache_bytes + data->bytes.size() > config.cache_bytes) {
            const auto victim = lru.back();
            const auto& removed = cache.at(victim).data;
            counters.cache_bytes -= removed->bytes.size();
            removed->resident = false;
            if (removed->speculative && !removed->used) {
                counters.unused_prefetch_cache_bytes -= removed->bytes.size();
                counters.wasted_prefetch_bytes += removed->bytes.size();
                removed->counted_wasted = true;
            }
            cache.erase(victim);
            lru.pop_back();
        }
        lru.push_front(id);
        try {
            cache.emplace(id, Entry{std::move(data), lru.begin()});
        } catch (...) {
            lru.pop_front();
            throw;
        }
        auto& admitted = cache.at(id).data;
        admitted->resident = true;
        counters.cache_bytes += admitted->bytes.size();
        if (admitted->speculative && !admitted->used) {
            counters.unused_prefetch_cache_bytes += admitted->bytes.size();
        }
        counters.peak_cache_bytes = std::max(counters.peak_cache_bytes, counters.cache_bytes);
    }

    void use(const Block& data) {
        std::lock_guard lock(mutex);
        if (data->speculative && !data->used) {
            data->used = true;
            counters.useful_prefetch_bytes += data->bytes.size();
            if (data->resident) {
                counters.unused_prefetch_cache_bytes -= data->bytes.size();
            }
            if (data->counted_wasted) {
                counters.wasted_prefetch_bytes -= data->bytes.size();
                data->counted_wasted = false;
            }
        }
    }

    void worker() {
        for (;;) {
            std::shared_ptr<Job> job;
            {
                std::unique_lock lock(mutex);
                changed.wait(lock, [this] {
                    return !demand_jobs.empty() ||
                           (!prefetch_jobs.empty() && active_prefetch < prefetch_limit()) ||
                           (stopping && prefetch_jobs.empty());
                });
                if (demand_jobs.empty() && prefetch_jobs.empty()) {
                    return;
                }
                if (!demand_jobs.empty()) {
                    job = std::move(demand_jobs.front());
                    demand_jobs.pop_front();
                } else {
                    job = std::move(prefetch_jobs.front());
                    prefetch_jobs.pop_front();
                    ++active_prefetch;
                    job->dispatched_background = true;
                }
                job->running = true;
                ++active;
                ++counters.remote_requests;
            }
            const auto start = Clock::now();
            const auto offset = job->first * block_size;
            const auto length = static_cast<std::size_t>(std::min<std::uint64_t>(
                job->promises.size() * block_size, source->size() - offset));
            std::vector<Block> blocks;
            std::exception_ptr error;
            std::chrono::nanoseconds backend_queue{};
            try {
                auto result = source->read(offset, length);
                backend_queue = result.backend_queue;
                if (result.data.size() != length) {
                    throw std::runtime_error("backend returned a short read");
                }
                for (std::size_t i = 0; i < result.data.size(); i += block_size) {
                    const auto end = std::min(i + block_size, result.data.size());
                    auto data = std::make_shared<Data>();
                    data->bytes.assign(result.data.begin() + i, result.data.begin() + end);
                    data->speculative = job->speculative;
                    blocks.push_back(std::move(data));
                }
            } catch (...) {
                error = std::current_exception();
            }
            const auto end = Clock::now();
            {
                std::lock_guard lock(mutex);
                const auto queue_us = std::chrono::duration<double, std::micro>(
                                          start - job->submitted + backend_queue)
                                          .count();
                const auto service_us =
                    std::chrono::duration<double, std::micro>(end - start - backend_queue).count();
                counters.queue_us += queue_us;
                counters.service_us += service_us;
                if (error) {
                    ++counters.failed_fetches;
                } else {
                    counters.remote_bytes += length;
                    if (job->speculative) {
                        counters.prefetch_bytes += length;
                    }
                    const auto bucket = length <= 16 * kib    ? 0
                                        : length <= 64 * kib  ? 1
                                        : length <= 256 * kib ? 2
                                                              : 3;
                    ++counters.fetch_size_counts[bucket];
                    const auto feedback_queue = config.queue_feedback == QueueFeedback::decomposed
                                                    ? backend_queue
                                                    : std::chrono::nanoseconds{};
                    policy.sample(
                        length,
                        std::chrono::duration<double, std::micro>(end - start - feedback_queue)
                            .count(),
                        std::chrono::duration<double, std::micro>(start - job->submitted +
                                                                  feedback_queue)
                            .count());
                }
                for (std::size_t i = 0; i < job->promises.size(); ++i) {
                    if (!error) {
                        try {
                            insert(job->first + i, blocks[i]);
                        } catch (...) {
                            // Cache admission failure must not invalidate successfully fetched
                            // data.
                            ++counters.cache_admission_failures;
                        }
                        job->promises[i]->set_value(blocks[i]);
                    } else {
                        job->promises[i]->set_exception(error);
                    }
                    flights.erase(job->first + i);
                }
                if (job->speculative) {
                    reserved_prefetch_blocks -= job->promises.size();
                }
                if (job->dispatched_background) {
                    --active_prefetch;
                }
                --active;
            }
            changed.notify_all();
        }
    }

    std::shared_future<Block> enqueue(std::uint64_t first, std::uint64_t last, std::uint64_t needed,
                                      bool speculative) {
        auto job = std::make_shared<Job>();
        job->first = first;
        job->submitted = Clock::now();
        job->speculative = job->background = speculative;
        std::shared_future<Block> result;
        try {
            for (auto current = first; current < last; ++current) {
                auto promise = std::make_shared<std::promise<Block>>();
                auto future = promise->get_future().share();
                if (current == needed) {
                    result = future;
                }
                job->promises.push_back(promise);
                flights.emplace(current, Flight{std::move(future), job});
            }
            (speculative ? prefetch_jobs : demand_jobs).push_back(std::move(job));
        } catch (...) {
            for (auto current = first; current < last; ++current) {
                flights.erase(current);
            }
            changed.notify_all();
            throw;
        }
        if (speculative) {
            reserved_prefetch_blocks += last - first;
            ++counters.prefetch_admissions;
        }
        counters.peak_inflight_bytes =
            std::max<std::uint64_t>(counters.peak_inflight_bytes, flights.size() * block_size);
        changed.notify_all();
        return result;
    }

    void promote(const Flight& flight) {
        const auto job = flight.job.lock();
        if (job && job->background && !job->running) {
            const auto found = std::find(prefetch_jobs.begin(), prefetch_jobs.end(), job);
            if (found != prefetch_jobs.end()) {
                demand_jobs.push_back(job);
                prefetch_jobs.erase(found);
                job->background = false;
                ++counters.promoted_prefetches;
                changed.notify_all();
            }
        }
    }

    Block block(std::uint64_t id, std::size_t fetch_bytes, bool& fetched) {
        std::unique_lock lock(mutex);
        bool counted = false;
        for (;;) {
            if (const auto it = cache.find(id); it != cache.end()) {
                if (!counted) {
                    ++counters.hit_blocks;
                }
                lru.splice(lru.begin(), lru, it->second.position);
                return it->second.data;
            }
            if (!counted) {
                ++counters.miss_blocks;
                counted = true;
            }
            if (const auto it = flights.find(id); it != flights.end()) {
                ++counters.shared_misses;
                promote(it->second);
                auto future = it->second.future;
                lock.unlock();
                return future.get();
            }
            const auto available = config.max_inflight_bytes / block_size - flights.size();
            if (available == 0) {
                changed.wait(lock);
                continue;
            }
            const auto window = fetch_bytes / block_size;
            const auto first_allowed = id / window * window;
            const auto image_blocks =
                source->size() / block_size + (source->size() % block_size != 0);
            const auto last_allowed = std::min<std::uint64_t>(first_allowed + window, image_blocks);
            auto first = id;
            auto last = id + 1;
            while (first > first_allowed && last - first < available &&
                   !cache.contains(first - 1) && !flights.contains(first - 1)) {
                --first;
            }
            while (last < last_allowed && last - first < available && !cache.contains(last) &&
                   !flights.contains(last)) {
                ++last;
            }
            auto needed = enqueue(first, last, id, false);
            fetched = true;
            lock.unlock();
            changed.notify_all();
            return needed.get();
        }
    }

    void prefetch(std::uint64_t stream, std::uint64_t request_end, std::size_t demand_bytes,
                  const FetchPlan& plan) {
        if (!prefetch_enabled() || !plan.sequential || plan.bytes < 64 * kib) {
            return;
        }
        std::lock_guard lock(mutex);
        if (prefetch_limit() == 0) {
            ++counters.prefetch_rejections;
            return;
        }
        auto frontier = policy.frontier(stream);
        const auto distance = frontier > request_end ? frontier - request_end : 0;
        if (distance > plan.bytes / 4 && distance <= 2 * plan.bytes) {
            return;
        }
        const auto aligned_end = (request_end - 1) / demand_bytes * demand_bytes + demand_bytes;
        const auto start =
            distance <= 2 * plan.bytes ? std::max(frontier, aligned_end) : aligned_end;
        if (start >= source->size()) {
            return;
        }
        auto window = plan.bytes;
        auto allowed = config.max_inflight_bytes / block_size - flights.size();
        if (congestion_control()) {
            const auto spec_limit = config.max_inflight_bytes / block_size / 2;
            allowed = std::min(allowed, spec_limit > reserved_prefetch_blocks
                                            ? spec_limit - reserved_prefetch_blocks
                                            : 0);
            if (!demand_jobs.empty() && policy.queue_us() > 2 * policy.fixed_us()) {
                ++counters.prefetch_rejections;
                return;
            }
        }
        if (config.policy == Policy::adaptive_v4) {
            const auto resolved = counters.useful_prefetch_bytes + counters.wasted_prefetch_bytes;
            const auto usefulness =
                resolved ? double(counters.useful_prefetch_bytes) / resolved : 1;
            const auto occupied =
                (counters.unused_prefetch_cache_bytes + block_size - 1) / block_size +
                reserved_prefetch_blocks;
            const auto quota = config.cache_bytes / block_size / 4;
            allowed = std::min(allowed, quota > occupied ? quota - occupied : 0);
            if (resolved >= 64 * kib && usefulness < 0.25) {
                // A bounded probe lets a new locality phase recover from historical waste.
                if (++low_usefulness_attempts % 16 != 0) {
                    ++counters.prefetch_rejections;
                    return;
                }
                window = std::min(window, 64 * kib);
            }
            if (usefulness < 0.5) {
                window = std::min(window, 64 * kib);
            }
        }
        if (allowed == 0) {
            ++counters.prefetch_rejections;
            return;
        }
        const auto bytes = std::min<std::uint64_t>(window, source->size() - start);
        const auto end_block = (start + bytes) / block_size + ((start + bytes) % block_size != 0);
        auto current = start / block_size;
        while (current < end_block) {
            if (cache.contains(current) || flights.contains(current)) {
                ++current;
                continue;
            }
            if (allowed == 0) {
                break;
            }
            const auto first = current++;
            while (current < end_block && current - first < allowed && !cache.contains(current) &&
                   !flights.contains(current)) {
                ++current;
            }
            enqueue(first, current, first, true);
            allowed -= current - first;
        }
        policy.frontier(stream, std::min<std::uint64_t>(current * block_size, source->size()));
    }
};

ImageReader::ImageReader(std::shared_ptr<RemoteBlockStore> source, Config config)
    : impl_(std::make_unique<Impl>(std::move(source), config)) {}

ImageReader::~ImageReader() = default;

Bytes ImageReader::read(std::uint64_t offset, std::size_t length, std::uint64_t stream_id) {
    check_range(impl_->source->size(), offset, length);
    FetchPlan plan;
    auto fetch_bytes = impl_->config.fetch_bytes;
    if (length && impl_->config.policy != Policy::fixed) {
        std::lock_guard lock(impl_->mutex);
        plan = impl_->policy.observe(stream_id, offset, length, impl_->source->size());
        fetch_bytes = impl_->prefetch_enabled() ? std::min(plan.bytes, 64 * kib) : plan.bytes;
    }
    Bytes output(length);
    std::size_t done = 0;
    bool fetched = false;
    while (done < length) {
        const auto absolute = offset + done;
        const auto data = impl_->block(absolute / block_size, fetch_bytes, fetched);
        impl_->use(data);
        const auto begin = static_cast<std::size_t>(absolute % block_size);
        const auto count = std::min(length - done, data->bytes.size() - begin);
        std::memcpy(output.data() + done, data->bytes.data() + begin, count);
        done += count;
    }
    {
        std::lock_guard lock(impl_->mutex);
        ++impl_->counters.foreground_reads;
        impl_->counters.logical_bytes += length;
        if (fetched) {
            impl_->policy.miss(stream_id, offset + length);
        }
    }
    if (length) {
        impl_->prefetch(stream_id, offset + length, fetch_bytes, plan);
    }
    return output;
}

void ImageReader::drain() {
    std::unique_lock lock(impl_->mutex);
    impl_->changed.wait(lock, [this] {
        return impl_->demand_jobs.empty() && impl_->prefetch_jobs.empty() && impl_->active == 0;
    });
}

Stats ImageReader::stats() const {
    std::lock_guard lock(impl_->mutex);
    auto result = impl_->counters;
    result.estimated_fixed_us = impl_->policy.fixed_us();
    result.estimated_bandwidth_mib_s = impl_->policy.bandwidth_mib_s();
    result.estimated_queue_us = impl_->policy.queue_us();
    return result;
}

} // namespace aio
