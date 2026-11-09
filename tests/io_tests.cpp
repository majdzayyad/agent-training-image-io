#include "adaptive_io/io.hpp"
#include "adaptive_io/policy.hpp"

#include <algorithm>
#include <atomic>
#include <barrier>
#include <cmath>
#include <condition_variable>
#include <future>
#include <iostream>
#include <mutex>
#include <random>
#include <stdexcept>
#include <thread>

namespace {

void require(bool condition, const char* message) {
    if (!condition) {
        throw std::runtime_error(message);
    }
}

template <typename F> void must_throw(F&& function) {
    bool threw = false;
    try {
        function();
    } catch (const std::exception&) {
        threw = true;
    }
    require(threw, "expected exception");
}

struct MemoryStore final : aio::RemoteBlockStore {
    aio::Bytes bytes;
    std::atomic<int> calls{0};
    std::atomic<int> failures{0};
    std::atomic<bool> short_read{false};
    std::chrono::milliseconds delay{0};
    std::chrono::nanoseconds reported_queue{};

    explicit MemoryStore(std::size_t size) : bytes(size) {
        std::mt19937 rng(12345);
        for (auto& byte : bytes) {
            byte = static_cast<std::byte>(rng() & 255);
        }
    }
    std::uint64_t size() const override {
        return bytes.size();
    }
    aio::FetchResult read(std::uint64_t offset, std::size_t length) override {
        aio::check_range(size(), offset, length);
        ++calls;
        std::this_thread::sleep_for(delay);
        auto remaining = failures.load();
        while (remaining > 0) {
            if (failures.compare_exchange_weak(remaining, remaining - 1)) {
                throw std::runtime_error("injected fetch failure");
            }
        }
        if (short_read && length) {
            --length;
        }
        return {aio::Bytes(bytes.begin() + offset, bytes.begin() + offset + length),
                reported_queue};
    }
};

void compare(aio::ImageReader& reader, const MemoryStore& store, std::uint64_t offset,
             std::size_t length, std::uint64_t stream = 0) {
    const auto result = reader.read(offset, length, stream);
    require(result ==
                aio::Bytes(store.bytes.begin() + offset, store.bytes.begin() + offset + length),
            "returned bytes do not match source");
}

void boundaries_and_eviction() {
    auto source = std::make_shared<MemoryStore>(2 * aio::mib + 719);
    for (const auto fetch : {16 * aio::kib, 64 * aio::kib, 256 * aio::kib, aio::mib}) {
        aio::Config config{32 * aio::kib, fetch, 3, 64 * aio::kib};
        aio::ImageReader reader(source, config);
        compare(reader, *source, 1, 1);
        compare(reader, *source, 4095, 8194);
        compare(reader, *source, source->size() - 717, 717);
        compare(reader, *source, source->size(), 0);
        compare(reader, *source, 73, 128 * aio::kib);
        must_throw([&] { reader.read(source->size(), 1); });
        must_throw([&] { reader.read(UINT64_MAX, 2); });
        reader.drain();
        require(reader.stats().peak_cache_bytes <= config.cache_bytes, "cache budget exceeded");
        require(reader.stats().peak_inflight_bytes <= config.max_inflight_bytes,
                "in-flight budget exceeded");
    }
}

void single_flight_and_hits() {
    auto source = std::make_shared<MemoryStore>(aio::mib);
    source->delay = std::chrono::milliseconds(20);
    aio::ImageReader reader(source, {aio::mib, 256 * aio::kib, 4, aio::mib});
    constexpr int clients = 16;
    std::barrier gate(clients);
    std::vector<std::future<void>> tasks;
    for (int i = 0; i < clients; ++i) {
        tasks.push_back(std::async(std::launch::async, [&, i] {
            gate.arrive_and_wait();
            compare(reader, *source, (i % 8) * aio::block_size, aio::block_size, i);
        }));
    }
    for (auto& task : tasks) {
        task.get();
    }
    require(source->calls == 1, "overlapping misses were not single-flight");
    compare(reader, *source, 0, 64 * aio::kib);
    require(source->calls == 1, "resident data was fetched again");
    require(reader.stats().shared_misses > 0, "test did not exercise shared misses");
}

void failure_recovery() {
    auto source = std::make_shared<MemoryStore>(aio::mib);
    source->failures = 1;
    aio::ImageReader reader(source, {64 * aio::kib, 16 * aio::kib, 2, 32 * aio::kib});
    must_throw([&] { reader.read(0, aio::block_size); });
    compare(reader, *source, 0, aio::block_size);
    source->short_read = true;
    must_throw([&] { reader.read(aio::mib / 2, aio::block_size); });
    source->short_read = false;
    compare(reader, *source, aio::mib / 2, aio::block_size);
    reader.drain();
    require(reader.stats().failed_fetches == 2, "fetch failures were not counted");
}

void concurrent_eviction() {
    auto source = std::make_shared<MemoryStore>(aio::mib + 131);
    aio::Config config{4 * aio::block_size, 64 * aio::kib, 4, 8 * aio::block_size};
    aio::ImageReader reader(source, config);
    std::vector<std::future<void>> tasks;
    for (int i = 0; i < 12; ++i) {
        tasks.push_back(std::async(std::launch::async, [&, i] {
            std::mt19937 rng(i);
            for (int j = 0; j < 80; ++j) {
                const auto offset = rng() % (source->size() - 9000);
                compare(reader, *source, offset, 9000, i);
            }
        }));
    }
    for (auto& task : tasks) {
        task.get();
    }
    reader.drain();
    require(reader.stats().peak_cache_bytes <= config.cache_bytes,
            "concurrent cache exceeded budget");
    require(reader.stats().peak_inflight_bytes <= config.max_inflight_bytes,
            "concurrent flights exceeded budget");
}

void adaptive_cost_and_streams() {
    aio::FetchPolicy cheap, expensive;
    for (int i = 0; i < 32; ++i) {
        const auto size = i % 2 ? 256 * aio::kib : 16 * aio::kib;
        const auto transfer_us = size / (64.0 * aio::mib) * 1e6;
        cheap.sample(size, 1 + transfer_us, 0);
        expensive.sample(size, 5000 + transfer_us, 0);
    }
    require(cheap.observe(1, 4096, 16384, 8 * aio::mib).bytes == 16 * aio::kib,
            "cheap requests should avoid unnecessary bytes");
    require(expensive.observe(1, 4096, 16384, 8 * aio::mib).bytes == 64 * aio::kib,
            "expensive requests should coalesce a crossing demand");
    aio::FetchPolicy interleaved;
    aio::FetchPlan first, second;
    for (int i = 0; i < 128; ++i) {
        first = interleaved.observe(10, i * aio::block_size, aio::block_size, 8 * aio::mib);
        second = interleaved.observe(20, 4 * aio::mib + i * aio::block_size, aio::block_size,
                                     8 * aio::mib);
    }
    require(first.sequential && second.sequential && first.bytes > 64 * aio::kib &&
                second.bytes > 64 * aio::kib,
            "interleaved streams lost independent sequential locality");
    require(!interleaved.observe(10, 7 * aio::mib, aio::block_size, 8 * aio::mib).sequential,
            "a random phase transition retained speculative confidence");
    require(
        interleaved.observe(20, 4 * aio::mib + 128 * aio::block_size, aio::block_size, 8 * aio::mib)
            .sequential,
        "one stream's phase transition contaminated another");
    for (int i = 0; i < 2048; ++i) {
        interleaved.observe(1000 + i, 0, aio::block_size, aio::mib);
    }
    require(interleaved.stream_count() <= 1024, "per-stream policy state is unbounded");
}

void all_ablation_modes() {
    auto source = std::make_shared<MemoryStore>(4 * aio::mib + 113);
    for (const auto mode : {aio::Policy::adaptive_v1, aio::Policy::adaptive_v2,
                            aio::Policy::adaptive_v3, aio::Policy::adaptive_v4}) {
        aio::Config config{128 * aio::kib, 256 * aio::kib, 3, 512 * aio::kib};
        config.policy = mode;
        aio::ImageReader reader(source, config);
        std::vector<std::future<void>> tasks;
        for (int stream = 0; stream < 4; ++stream) {
            tasks.push_back(std::async(std::launch::async, [&, stream] {
                for (int index = 0; index < 256; ++index) {
                    compare(reader, *source, stream * aio::mib + index * aio::block_size,
                            aio::block_size, stream);
                }
                compare(reader, *source, source->size() - 115, 115, stream);
            }));
        }
        for (auto& task : tasks) {
            task.get();
        }
        reader.drain();
        const auto stats = reader.stats();
        require(stats.peak_cache_bytes <= config.cache_bytes, "adaptive cache exceeded budget");
        require(stats.peak_inflight_bytes <= config.max_inflight_bytes,
                "adaptive in-flight payload exceeded budget");
        require(stats.useful_prefetch_bytes + stats.wasted_prefetch_bytes +
                        stats.unused_prefetch_cache_bytes ==
                    stats.prefetch_bytes,
                "prefetch byte lifecycle accounting is inconsistent");
        if (mode == aio::Policy::adaptive_v1) {
            require(stats.prefetch_bytes == 0, "V1 unexpectedly submitted speculative I/O");
        }
        if (mode == aio::Policy::adaptive_v4) {
            require(stats.unused_prefetch_cache_bytes <= config.cache_bytes / 4,
                    "speculative tail blocks exceeded the resident admission quota");
        }
    }
}

struct GateStore final : aio::RemoteBlockStore {
    MemoryStore source{8 * aio::mib};
    std::mutex mutex;
    std::condition_variable changed;
    bool entered = false;
    bool released = false;

    std::uint64_t size() const override {
        return source.size();
    }
    aio::FetchResult read(std::uint64_t offset, std::size_t length) override {
        if (length > 64 * aio::kib) {
            std::unique_lock lock(mutex);
            entered = true;
            changed.notify_all();
            changed.wait(lock, [&] { return released; });
        }
        return source.read(offset, length);
    }
    bool wait_for_speculation() {
        std::unique_lock lock(mutex);
        return changed.wait_for(lock, std::chrono::seconds(3), [&] { return entered; });
    }
    void release() {
        std::lock_guard lock(mutex);
        released = true;
        changed.notify_all();
    }
};

void foreground_reservation() {
    auto source = std::make_shared<GateStore>();
    aio::Config config{4 * aio::mib, 256 * aio::kib, 2, 4 * aio::mib};
    config.policy = aio::Policy::adaptive_v3;
    aio::ImageReader reader(source, config);
    auto sequential = std::async(std::launch::async, [&] {
        for (int i = 0; i < 256; ++i) {
            compare(reader, source->source, i * aio::block_size, aio::block_size, 1);
        }
    });
    const bool speculative_in_flight = source->wait_for_speculation();
    auto foreground = std::async(std::launch::async, [&] {
        compare(reader, source->source, 7 * aio::mib + 123, aio::block_size, 2);
    });
    const bool independent_demand_completed =
        foreground.wait_for(std::chrono::seconds(2)) == std::future_status::ready;
    source->release();
    foreground.get();
    sequential.get();
    reader.drain();
    require(speculative_in_flight, "test did not exercise a blocked speculative transfer");
    require(independent_demand_completed,
            "speculative transfer blocked independent foreground work");
    require(reader.stats().prefetch_bytes > 0, "asynchronous prefetch was not exercised");
}

void shared_failure_and_recovery() {
    auto source = std::make_shared<MemoryStore>(aio::mib);
    source->delay = std::chrono::milliseconds(50);
    source->failures = 1;
    aio::ImageReader reader(source);
    std::barrier gate(8);
    std::vector<std::future<void>> clients;
    for (int i = 0; i < 8; ++i) {
        clients.push_back(std::async(std::launch::async, [&] {
            gate.arrive_and_wait();
            must_throw([&] { reader.read(0, aio::block_size); });
        }));
    }
    for (auto& client : clients) {
        client.get();
    }
    require(source->calls == 1, "failed overlapping fetches were duplicated");
    compare(reader, *source, 0, aio::block_size);
    reader.drain();
}

void prefetch_recovers_after_waste() {
    auto source = std::make_shared<MemoryStore>(16 * aio::mib);
    aio::Config config{256 * aio::kib, 256 * aio::kib, 2, aio::mib};
    config.policy = aio::Policy::adaptive_v4;
    aio::ImageReader reader(source, config);
    for (int i = 0; i < 5; ++i) {
        compare(reader, *source, i * 64 * aio::kib, 64 * aio::kib, 1);
    }
    reader.drain();
    for (int i = 2; i < 6; ++i) {
        compare(reader, *source, i * aio::mib, 64 * aio::kib, 2);
    }
    const auto abandoned = reader.stats();
    require(abandoned.wasted_prefetch_bytes >= 64 * aio::kib &&
                abandoned.useful_prefetch_bytes == 0,
            "test did not create an entirely unused, evicted prefetch");
    for (int i = 0; i < 40; ++i) {
        compare(reader, *source, 6 * aio::mib + i * 64 * aio::kib, 64 * aio::kib, 3);
    }
    reader.drain();
    require(reader.stats().prefetch_admissions > abandoned.prefetch_admissions &&
                reader.stats().useful_prefetch_bytes > 0,
            "historical waste permanently disabled prefetch in a new sequential phase");
}

void unrepresentable_backend_costs() {
    auto source = std::make_shared<MemoryStore>(64 * aio::kib);
    aio::ControlledStore too_slow(source, 0, 1e-20);
    must_throw([&] { too_slow.read(0, aio::block_size); });
    require(source->calls == 0, "unrepresentable cost reached the backend");
    must_throw([&] { aio::ControlledStore too_large(source, 0, 1e308); });
}

void refill_control_variants() {
    aio::FetchPolicy grow(aio::mib, aio::RefillRule::grow_reset);
    require(grow.observe(1, 0, aio::block_size, 8 * aio::mib).bytes == 16 * aio::kib,
            "grow/reset must start small");
    grow.miss(1, aio::block_size);
    require(grow.observe(1, aio::block_size, aio::block_size, 8 * aio::mib).bytes == 32 * aio::kib,
            "a missed demand did not grow the next window");
    require(grow.observe(1, 2 * aio::block_size, aio::block_size, 8 * aio::mib).bytes ==
                32 * aio::kib,
            "a cache hit must not grow the window");
    require(grow.observe(1, aio::mib, aio::block_size, 8 * aio::mib).bytes == 16 * aio::kib,
            "discontinuity did not reset the window");
    for (int i = 0; i < 16; ++i) {
        const auto offset = aio::mib + i * aio::block_size;
        grow.observe(1, offset, aio::block_size, 8 * aio::mib);
        grow.miss(1, offset + aio::block_size);
    }
    require(grow.observe(1, aio::mib + 16 * aio::block_size, aio::block_size, 8 * aio::mib).bytes ==
                aio::mib,
            "grow/reset did not saturate at the hard limit");

    aio::FetchPolicy aware, agnostic(aio::mib, aio::RefillRule::alignment_agnostic);
    aio::FetchPolicy frozen(aio::mib, aio::RefillRule::fixed_cost);
    aio::FetchPolicy locality(aio::mib, aio::RefillRule::locality_only);
    for (int i = 0; i < 32; ++i) {
        const auto bytes = i % 2 ? 256 * aio::kib : 16 * aio::kib;
        const auto service = 5000 + bytes / (64.0 * aio::mib) * 1e6;
        aware.sample(bytes, service, 1000);
        agnostic.sample(bytes, service, 1000);
        frozen.sample(bytes, service, 1000);
        locality.sample(bytes, service, 1000);
    }
    require(aware.observe(2, 4096, 16384, 8 * aio::mib).bytes == 64 * aio::kib &&
                agnostic.observe(2, 4096, 16384, 8 * aio::mib).bytes == 16 * aio::kib,
            "alignment ablation did not remove the window-crossing cost");
    require(frozen.fixed_us() == 200 && frozen.bandwidth_mib_s() == 256 && frozen.queue_us() == 0,
            "fixed-cost ablation learned service costs");
    require(locality.observe(3, 0, aio::block_size, 8 * aio::mib).bytes == 16 * aio::kib,
            "locality-only policy started with unsupported continuation");
    for (int i = 1; i < 128; ++i) {
        locality.observe(3, i * aio::block_size, aio::block_size, 8 * aio::mib);
    }
    require(locality.observe(3, 128 * aio::block_size, aio::block_size, 8 * aio::mib).bytes ==
                aio::mib,
            "locality-only policy did not follow sustained continuation");
    for (int i = 0; i < 160; ++i) {
        const auto bytes = i % 2 ? 256 * aio::kib : 16 * aio::kib;
        aware.sample(bytes, 1 + bytes / (64.0 * aio::mib) * 1e6, 0);
    }
    require(aware.fixed_us() < 10, "online regression did not adapt to changed service costs");

    auto source = std::make_shared<MemoryStore>(4 * aio::mib + 113);
    for (const auto rule : {aio::RefillRule::grow_reset, aio::RefillRule::fixed_cost,
                            aio::RefillRule::locality_only, aio::RefillRule::alignment_agnostic}) {
        aio::Config config{32 * aio::kib, 256 * aio::kib, 2, 64 * aio::kib};
        config.policy = aio::Policy::adaptive_v1;
        config.refill_rule = rule;
        aio::ImageReader reader(source, config);
        for (int i = 0; i < 48; ++i) {
            compare(reader, *source, (i % 3) * aio::mib + i * aio::block_size + 1, 16 * aio::kib,
                    i % 3);
        }
        compare(reader, *source, source->size() - 115, 115);
        reader.drain();
        const auto stats = reader.stats();
        require(stats.prefetch_bytes == 0 && stats.failed_fetches == 0 &&
                    stats.cache_admission_failures == 0 &&
                    stats.peak_cache_bytes <= config.cache_bytes &&
                    stats.peak_inflight_bytes <= config.max_inflight_bytes,
                "refill-only variant changed correctness, prefetch, or resource bounds");
    }
}

void queue_feedback_is_estimator_only() {
    for (const auto feedback :
         {aio::QueueFeedback::decomposed, aio::QueueFeedback::observable_only}) {
        auto source = std::make_shared<MemoryStore>(aio::mib);
        source->delay = std::chrono::milliseconds(30);
        source->reported_queue = std::chrono::milliseconds(20);
        aio::Config config;
        config.policy = aio::Policy::adaptive_v1;
        config.queue_feedback = feedback;
        aio::ImageReader reader(source, config);
        compare(reader, *source, 0, aio::block_size);
        reader.drain();
        const auto stats = reader.stats();
        const auto hidden = feedback == aio::QueueFeedback::observable_only ? 20000 : 0;
        require(std::abs(stats.estimated_queue_us - 0.2 * (stats.queue_us - hidden)) < 1e-6,
                "queue decomposition was not selectively withheld from the estimator");
        require(stats.remote_requests == 1 && stats.remote_bytes == 16 * aio::kib,
                "queue feedback changed the backend read contract");
    }
}

} // namespace

int main() {
    try {
        boundaries_and_eviction();
        single_flight_and_hits();
        failure_recovery();
        concurrent_eviction();
        adaptive_cost_and_streams();
        all_ablation_modes();
        foreground_reservation();
        shared_failure_and_recovery();
        prefetch_recovers_after_waste();
        unrepresentable_backend_costs();
        refill_control_variants();
        queue_feedback_is_estimator_only();
        std::cout << "cache, policy, priority, boundary, concurrency, and recovery tests passed\n";
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
