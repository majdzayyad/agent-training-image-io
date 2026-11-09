#include "adaptive_io/io.hpp"

#include <algorithm>
#include <charconv>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <latch>
#include <map>
#include <mutex>
#include <numeric>
#include <sstream>
#include <stdexcept>
#include <string_view>
#include <sys/resource.h>
#include <thread>

namespace {

std::uint64_t integer(std::string_view text) {
    std::uint64_t value = 0;
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (error != std::errc{} || end != text.data() + text.size()) {
        throw std::invalid_argument("invalid nonnegative integer: " + std::string(text));
    }
    return value;
}

struct Request {
    std::uint64_t offset;
    std::size_t length;
    std::uint64_t delay_us;
};

using Trace = std::map<std::uint64_t, std::vector<Request>>;

Trace load_trace(const std::string& path, std::uint64_t size) {
    std::ifstream input(path);
    std::string line;
    if (!input || !std::getline(input, line)) {
        throw std::runtime_error("could not read trace");
    }
    if (!line.empty() && line.back() == '\r') {
        line.pop_back();
    }
    if (line != "stream_id,offset,length,delay_us") {
        throw std::runtime_error("trace must start with stream_id,offset,length,delay_us");
    }
    Trace trace;
    while (std::getline(input, line)) {
        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }
        if (line.empty()) {
            continue;
        }
        std::vector<std::uint64_t> values;
        std::istringstream row(line);
        std::string field;
        while (std::getline(row, field, ',')) {
            values.push_back(integer(field));
        }
        if (values.size() != 4 || line.back() == ',' || values[2] == 0 ||
            values[2] > 16 * aio::mib || values[3] > 60'000'000) {
            throw std::runtime_error("invalid trace row: " + line);
        }
        aio::check_range(size, values[1], values[2]);
        trace[values[0]].push_back({values[1], static_cast<std::size_t>(values[2]), values[3]});
    }
    if (trace.empty() || trace.size() > 256) {
        throw std::runtime_error("trace needs 1..256 nonempty logical streams");
    }
    return trace;
}

double percentile(const std::vector<double>& sorted, double p) {
    return sorted[static_cast<std::size_t>(std::ceil(p * sorted.size())) - 1];
}

double real_number(const std::string& text) {
    std::size_t count = 0;
    const auto number = std::stod(text, &count);
    if (count != text.size() || !std::isfinite(number) || number < 0) {
        throw std::invalid_argument("invalid nonnegative real number");
    }
    return number;
}

} // namespace

int main(int argc, char** argv) {
    try {
        std::map<std::string, std::string> args{{"--policy", "fixed256"},
                                                {"--cache-mib", "64"},
                                                {"--workers", "4"},
                                                {"--inflight-mib", "8"},
                                                {"--latency-us", "0"},
                                                {"--bandwidth-mib", "0"},
                                                {"--source", ""},
                                                {"--trace", ""},
                                                {"--output", ""},
                                                {"--refill-rule", "cost"},
                                                {"--queue-feedback", "decomposed"},
                                                {"--stream-identity", "logical"},
                                                {"--frontend", "concurrent"}};
        for (int i = 1; i < argc; ++i) {
            const std::string key = argv[i];
            if (key == "--help") {
                std::cout << "replay --source FILE_OR_HTTP_URL --trace CSV "
                             "[--policy fixed16|fixed64|fixed256|fixed1024|local|"
                             "adaptive-v1|adaptive-v2|adaptive-v3|adaptive] "
                             "[--cache-mib 64] [--workers 4] [--inflight-mib 8] "
                             "[--latency-us 500] [--bandwidth-mib 128] [--output JSON] "
                             "[--refill-rule "
                             "cost|grow-reset|fixed-cost|locality-only|alignment-agnostic] "
                             "[--queue-feedback decomposed|observable-only] "
                             "[--stream-identity logical|collapsed] "
                             "[--frontend concurrent|serialized]\n";
                return 0;
            }
            if (!args.contains(key) || i + 1 == argc) {
                throw std::invalid_argument("unknown or incomplete argument: " + key);
            }
            args[key] = argv[++i];
        }
        if (args["--source"].empty() || args["--trace"].empty()) {
            throw std::invalid_argument("--source and --trace are required");
        }
        const std::string policy = args["--policy"];
        const std::map<std::string, std::size_t> fixed{{"fixed16", 16 * aio::kib},
                                                       {"fixed64", 64 * aio::kib},
                                                       {"fixed256", 256 * aio::kib},
                                                       {"fixed1024", aio::mib},
                                                       {"local", 0},
                                                       {"adaptive-v1", 256 * aio::kib},
                                                       {"adaptive-v2", 256 * aio::kib},
                                                       {"adaptive-v3", 256 * aio::kib},
                                                       {"adaptive", 256 * aio::kib}};
        if (!fixed.contains(policy)) {
            throw std::invalid_argument("unknown policy");
        }
        const auto& source_name = args["--source"];
        const auto http = source_name.starts_with("http://") || source_name.starts_with("https://");
        std::shared_ptr<aio::RemoteBlockStore> backend;
        if (http) {
            backend = std::make_shared<aio::HttpStore>(source_name);
        } else {
            backend = std::make_shared<aio::FileStore>(source_name);
        }
        const auto latency = real_number(args["--latency-us"]);
        const auto bandwidth = real_number(args["--bandwidth-mib"]);
        if (policy == "local" && (http || latency != 0 || bandwidth != 0)) {
            throw std::invalid_argument("local reference requires an unshaped local file");
        }
        if (latency > 0 || bandwidth > 0) {
            backend = std::make_shared<aio::ControlledStore>(backend, latency, bandwidth);
        }
        const auto cache_mib = integer(args["--cache-mib"]);
        const auto inflight_mib = integer(args["--inflight-mib"]);
        if (cache_mib == 0 || cache_mib > 16384 || inflight_mib == 0 || inflight_mib > 1024) {
            throw std::invalid_argument("cache must be 1..16384 MiB; in-flight budget 1..1024 MiB");
        }
        aio::Config config{cache_mib * aio::mib, fixed.at(policy), integer(args["--workers"]),
                           inflight_mib * aio::mib};
        const std::map<std::string, aio::Policy> adaptive{{"adaptive-v1", aio::Policy::adaptive_v1},
                                                          {"adaptive-v2", aio::Policy::adaptive_v2},
                                                          {"adaptive-v3", aio::Policy::adaptive_v3},
                                                          {"adaptive", aio::Policy::adaptive_v4}};
        if (adaptive.contains(policy)) {
            config.policy = adaptive.at(policy);
        }
        const std::map<std::string, aio::RefillRule> rules{
            {"cost", aio::RefillRule::cost},
            {"grow-reset", aio::RefillRule::grow_reset},
            {"fixed-cost", aio::RefillRule::fixed_cost},
            {"locality-only", aio::RefillRule::locality_only},
            {"alignment-agnostic", aio::RefillRule::alignment_agnostic}};
        if (!rules.contains(args["--refill-rule"]) ||
            (args["--refill-rule"] != "cost" && policy != "adaptive-v1")) {
            throw std::invalid_argument("refill-rule variants require adaptive-v1");
        }
        config.refill_rule = rules.at(args["--refill-rule"]);
        if (args["--queue-feedback"] == "observable-only") {
            config.queue_feedback = aio::QueueFeedback::observable_only;
        } else if (args["--queue-feedback"] != "decomposed") {
            throw std::invalid_argument("unknown queue feedback");
        }
        const bool collapsed = args["--stream-identity"] == "collapsed";
        const bool serialized = args["--frontend"] == "serialized";
        if ((!collapsed && args["--stream-identity"] != "logical") ||
            (!serialized && args["--frontend"] != "concurrent")) {
            throw std::invalid_argument("unknown stream identity or frontend mode");
        }
        std::mutex frontend;
        std::unique_ptr<aio::ImageReader> reader;
        if (policy != "local") {
            reader = std::make_unique<aio::ImageReader>(backend, config);
        }
        const auto trace = load_trace(args["--trace"], backend->size());
        std::vector<std::vector<double>> samples(trace.size());
        std::vector<std::uint64_t> hashes(trace.size(), 14695981039346656037ULL);
        std::vector<std::exception_ptr> errors(trace.size());
        std::latch start(1);
        std::latch ready(trace.size());
        std::vector<std::jthread> clients;
        std::size_t index = 0;
        std::uint64_t logical_bytes = 0;
        try {
            for (const auto& [stream, requests] : trace) {
                const auto slot = index++;
                for (const auto& request : requests) {
                    logical_bytes += request.length;
                }
                clients.emplace_back([&, slot, stream, requests = &requests] {
                    ready.count_down();
                    start.wait();
                    try {
                        samples[slot].reserve(requests->size());
                        for (std::size_t r = 0; r < requests->size(); ++r) {
                            const auto& request = (*requests)[r];
                            const auto before = aio::Clock::now();
                            const auto fetch = [&] {
                                return reader ? reader->read(request.offset, request.length,
                                                             collapsed ? 0 : stream)
                                              : backend->read(request.offset, request.length).data;
                            };
                            aio::Bytes data;
                            if (serialized) {
                                std::lock_guard lock(frontend);
                                data = fetch();
                            } else {
                                data = fetch();
                            }
                            samples[slot].push_back(std::chrono::duration<double, std::micro>(
                                                        aio::Clock::now() - before)
                                                        .count());
                            for (const auto byte : data) {
                                hashes[slot] ^= std::to_integer<unsigned char>(byte);
                                hashes[slot] *= 1099511628211ULL;
                            }
                            if (r + 1 < requests->size()) {
                                std::this_thread::sleep_for(
                                    std::chrono::microseconds(request.delay_us));
                            }
                        }
                    } catch (...) {
                        errors[slot] = std::current_exception();
                    }
                });
            }
        } catch (...) {
            start.count_down();
            throw;
        }
        ready.wait();
        const auto before = aio::Clock::now();
        start.count_down();
        clients.clear();
        const auto finished = aio::Clock::now();
        for (const auto& error : errors) {
            if (error) {
                std::rethrow_exception(error);
            }
        }
        if (reader) {
            reader->drain();
        }
        const auto drained = aio::Clock::now();
        const auto elapsed = std::chrono::duration<double>(finished - before).count();
        const auto drain_ms = std::chrono::duration<double, std::milli>(drained - finished).count();
        std::vector<double> all_samples;
        for (auto& stream_samples : samples) {
            all_samples.insert(all_samples.end(), stream_samples.begin(), stream_samples.end());
        }
        std::sort(all_samples.begin(), all_samples.end());
        std::uint64_t checksum = 0;
        for (const auto hash : hashes) {
            checksum = (checksum * 1099511628211ULL) ^ hash;
        }
        const auto stats = reader ? reader->stats() : aio::Stats{};
        struct rusage usage {};
        getrusage(RUSAGE_SELF, &usage);
        std::ostringstream json;
        json << std::setprecision(12) << "{\n"
             << "  \"policy\": \"" << policy << "\",\n"
             << "  \"refill_rule\": \"" << args["--refill-rule"] << "\",\n"
             << "  \"queue_feedback\": \"" << args["--queue-feedback"] << "\",\n"
             << "  \"stream_identity\": \"" << args["--stream-identity"] << "\",\n"
             << "  \"frontend\": \"" << args["--frontend"] << "\",\n"
             << "  \"clients\": " << trace.size() << ",\n"
             << "  \"reads\": " << all_samples.size() << ",\n"
             << "  \"elapsed_s\": " << elapsed << ",\n"
             << "  \"drain_ms\": " << drain_ms << ",\n"
             << "  \"p50_us\": " << percentile(all_samples, 0.50) << ",\n"
             << "  \"p95_us\": " << percentile(all_samples, 0.95) << ",\n"
             << "  \"p99_us\": " << percentile(all_samples, 0.99) << ",\n"
             << "  \"logical_bytes\": " << logical_bytes << ",\n"
             << "  \"throughput_mib_s\": " << logical_bytes / elapsed / aio::mib << ",\n"
             << "  \"remote_bytes\": " << stats.remote_bytes << ",\n"
             << "  \"remote_requests\": " << stats.remote_requests << ",\n"
             << "  \"failed_fetches\": " << stats.failed_fetches << ",\n"
             << "  \"read_amplification\": "
             << static_cast<double>(stats.remote_bytes) / logical_bytes << ",\n"
             << "  \"hit_blocks\": " << stats.hit_blocks << ",\n"
             << "  \"miss_blocks\": " << stats.miss_blocks << ",\n"
             << "  \"shared_misses\": " << stats.shared_misses << ",\n"
             << "  \"mean_queue_us\": "
             << (stats.remote_requests ? stats.queue_us / stats.remote_requests : 0) << ",\n"
             << "  \"mean_service_us\": "
             << (stats.remote_requests ? stats.service_us / stats.remote_requests : 0) << ",\n"
             << "  \"peak_cache_bytes\": " << stats.peak_cache_bytes << ",\n"
             << "  \"peak_inflight_bytes\": " << stats.peak_inflight_bytes << ",\n"
             << "  \"prefetch_bytes\": " << stats.prefetch_bytes << ",\n"
             << "  \"useful_prefetch_bytes\": " << stats.useful_prefetch_bytes << ",\n"
             << "  \"wasted_prefetch_bytes\": " << stats.wasted_prefetch_bytes << ",\n"
             << "  \"unused_prefetch_cache_bytes\": " << stats.unused_prefetch_cache_bytes << ",\n"
             << "  \"prefetch_admissions\": " << stats.prefetch_admissions << ",\n"
             << "  \"prefetch_rejections\": " << stats.prefetch_rejections << ",\n"
             << "  \"promoted_prefetches\": " << stats.promoted_prefetches << ",\n"
             << "  \"prefetch_usefulness\": "
             << (stats.prefetch_bytes ? double(stats.useful_prefetch_bytes) / stats.prefetch_bytes
                                      : 0)
             << ",\n"
             << "  \"fetch_le16k_count\": " << stats.fetch_size_counts[0] << ",\n"
             << "  \"fetch_le64k_count\": " << stats.fetch_size_counts[1] << ",\n"
             << "  \"fetch_le256k_count\": " << stats.fetch_size_counts[2] << ",\n"
             << "  \"fetch_le1024k_count\": " << stats.fetch_size_counts[3] << ",\n"
             << "  \"estimated_fixed_us\": " << stats.estimated_fixed_us << ",\n"
             << "  \"estimated_bandwidth_mib_s\": " << stats.estimated_bandwidth_mib_s << ",\n"
             << "  \"estimated_queue_us\": " << stats.estimated_queue_us << ",\n"
             << "  \"cache_admission_failures\": " << stats.cache_admission_failures << ",\n"
             << "  \"peak_rss_kib\": " << usage.ru_maxrss << ",\n"
             << "  \"process_output_blocks\": " << usage.ru_oublock << ",\n"
             << "  \"checksum\": \"" << std::hex << checksum << "\"\n}\n";
        if (!args["--output"].empty()) {
            std::ofstream output(args["--output"]);
            if (!(output << json.str())) {
                throw std::runtime_error("could not write result");
            }
        }
        std::cout << json.str();
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "replay: " << error.what() << '\n';
        return 1;
    }
}
