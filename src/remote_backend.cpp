#include "adaptive_io/io.hpp"

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <charconv>
#include <cmath>
#include <cstring>
#include <curl/curl.h>
#include <fcntl.h>
#include <limits>
#include <mutex>
#include <stdexcept>
#include <string_view>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>

namespace aio {

void check_range(std::uint64_t size, std::uint64_t offset, std::size_t length) {
    if (offset > size || length > size - offset) {
        throw std::out_of_range("read exceeds immutable image");
    }
}

FileStore::FileStore(const std::string& path) {
    fd_ = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
    if (fd_ < 0) {
        throw std::runtime_error("open: " + std::string(std::strerror(errno)));
    }
    struct stat st {};
    if (::fstat(fd_, &st) || !S_ISREG(st.st_mode) || st.st_size < 0) {
        ::close(fd_);
        throw std::runtime_error("source must be a regular immutable file");
    }
    size_ = static_cast<std::uint64_t>(st.st_size);
}

FileStore::~FileStore() {
    ::close(fd_);
}

std::uint64_t FileStore::size() const {
    return size_;
}

FetchResult FileStore::read(std::uint64_t offset, std::size_t length) {
    check_range(size_, offset, length);
    FetchResult result{Bytes(length), {}};
    std::size_t done = 0;
    while (done < length) {
        const auto n = ::pread(fd_, result.data.data() + done, length - done,
                               static_cast<off_t>(offset + done));
        if (n < 0 && errno == EINTR) {
            continue;
        }
        if (n <= 0) {
            throw std::runtime_error("short or failed file read");
        }
        done += static_cast<std::size_t>(n);
    }
    return result;
}

namespace {

struct CurlGlobal {
    CurlGlobal() {
        if (curl_global_init(CURL_GLOBAL_DEFAULT) != CURLE_OK) {
            throw std::runtime_error("curl_global_init failed");
        }
    }
    ~CurlGlobal() {
        curl_global_cleanup();
    }
};

struct CurlHandle {
    CURL* handle = nullptr;
    CurlHandle() {
        static CurlGlobal global;
        handle = curl_easy_init();
        if (!handle) {
            throw std::runtime_error("curl_easy_init failed");
        }
    }
    ~CurlHandle() {
        curl_easy_cleanup(handle);
    }
};

struct Response {
    Bytes data;
    std::size_t limit = 0;
    std::string etag;
    std::string range;
};

std::string trim(std::string_view value) {
    const auto first = value.find_first_not_of(" \t\r\n");
    const auto last = value.find_last_not_of(" \t\r\n");
    return first == std::string_view::npos ? ""
                                           : std::string(value.substr(first, last - first + 1));
}

std::size_t header_callback(char* data, std::size_t size, std::size_t count, void* context) {
    const auto length = size * count;
    auto& response = *static_cast<Response*>(context);
    try {
        std::string_view line(data, length);
        const auto colon = line.find(':');
        if (colon != std::string_view::npos) {
            std::string name(line.substr(0, colon));
            std::transform(name.begin(), name.end(), name.begin(),
                           [](unsigned char ch) { return static_cast<char>(std::tolower(ch)); });
            if (name == "etag") {
                response.etag = trim(line.substr(colon + 1));
            } else if (name == "content-range") {
                response.range = trim(line.substr(colon + 1));
            }
        }
        return length;
    } catch (...) {
        return 0;
    }
}

std::size_t body_callback(char* data, std::size_t size, std::size_t count, void* context) {
    auto& response = *static_cast<Response*>(context);
    if (size && count > std::numeric_limits<std::size_t>::max() / size) {
        return 0;
    }
    const auto length = size * count;
    if (length > response.limit - response.data.size()) {
        return 0;
    }
    try {
        const auto* begin = reinterpret_cast<std::byte*>(data);
        response.data.insert(response.data.end(), begin, begin + length);
        return length;
    } catch (...) {
        return 0;
    }
}

void prepare(CURL* handle, const std::string& url, Response& response) {
    curl_easy_reset(handle);
    curl_easy_setopt(handle, CURLOPT_URL, url.c_str());
    curl_easy_setopt(handle, CURLOPT_PROTOCOLS_STR, "http,https");
    curl_easy_setopt(handle, CURLOPT_FOLLOWLOCATION, 0L);
    curl_easy_setopt(handle, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(handle, CURLOPT_CONNECTTIMEOUT_MS, 5000L);
    curl_easy_setopt(handle, CURLOPT_TIMEOUT_MS, 30000L);
    curl_easy_setopt(handle, CURLOPT_ACCEPT_ENCODING, "identity");
    curl_easy_setopt(handle, CURLOPT_HEADERFUNCTION, header_callback);
    curl_easy_setopt(handle, CURLOPT_HEADERDATA, &response);
    curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, body_callback);
    curl_easy_setopt(handle, CURLOPT_WRITEDATA, &response);
}

long perform(CURL* handle) {
    const auto code = curl_easy_perform(handle);
    if (code != CURLE_OK) {
        throw std::runtime_error("HTTP read failed: " + std::string(curl_easy_strerror(code)));
    }
    long status = 0;
    curl_easy_getinfo(handle, CURLINFO_RESPONSE_CODE, &status);
    return status;
}

} // namespace

HttpStore::HttpStore(std::string url) : url_(std::move(url)) {
    CurlHandle handle;
    Response response;
    prepare(handle.handle, url_, response);
    curl_easy_setopt(handle.handle, CURLOPT_NOBODY, 1L);
    if (perform(handle.handle) != 200) {
        throw std::runtime_error("HTTP HEAD must return 200");
    }
    curl_off_t length = -1;
    curl_easy_getinfo(handle.handle, CURLINFO_CONTENT_LENGTH_DOWNLOAD_T, &length);
    if (length < 0 || response.etag.size() < 2 || response.etag.front() != '"' ||
        response.etag.back() != '"' || response.etag.find('\r') != std::string::npos ||
        response.etag.find('\n') != std::string::npos) {
        throw std::runtime_error("HTTP source needs Content-Length and a strong ETag");
    }
    size_ = static_cast<std::uint64_t>(length);
    etag_ = response.etag;
}

std::uint64_t HttpStore::size() const {
    return size_;
}

FetchResult HttpStore::read(std::uint64_t offset, std::size_t length) {
    check_range(size_, offset, length);
    if (length == 0) {
        return {};
    }
    thread_local CurlHandle handle;
    Response response;
    response.limit = length;
    response.data.reserve(length);
    prepare(handle.handle, url_, response);
    const auto range = std::to_string(offset) + "-" + std::to_string(offset + length - 1);
    const auto expected = "bytes " + range + "/" + std::to_string(size_);
    curl_easy_setopt(handle.handle, CURLOPT_RANGE, range.c_str());
    const auto match = "If-Match: " + etag_;
    std::unique_ptr<curl_slist, decltype(&curl_slist_free_all)> headers(
        curl_slist_append(nullptr, match.c_str()), curl_slist_free_all);
    if (!headers) {
        throw std::bad_alloc();
    }
    curl_easy_setopt(handle.handle, CURLOPT_HTTPHEADER, headers.get());
    const auto status = perform(handle.handle);
    if (status != 206 || response.range != expected || response.etag != etag_ ||
        response.data.size() != length) {
        throw std::runtime_error("HTTP range, length, or immutable ETag mismatch");
    }
    return {std::move(response.data), {}};
}

struct ControlledStore::Impl {
    std::shared_ptr<RemoteBlockStore> source;
    std::chrono::nanoseconds latency;
    double bytes_per_second;
    std::mutex mutex;
    Clock::time_point next_transfer{};
};

ControlledStore::ControlledStore(std::shared_ptr<RemoteBlockStore> source, double latency_us,
                                 double bandwidth_mib_s)
    : impl_(std::make_unique<Impl>()) {
    if (!source || !std::isfinite(latency_us) || latency_us < 0 ||
        !std::isfinite(bandwidth_mib_s) || bandwidth_mib_s < 0 ||
        !std::isfinite(bandwidth_mib_s * mib) || latency_us > 1e9) {
        throw std::invalid_argument("invalid controlled-backend costs");
    }
    impl_->source = std::move(source);
    impl_->latency = std::chrono::duration_cast<std::chrono::nanoseconds>(
        std::chrono::duration<double, std::micro>(latency_us));
    impl_->bytes_per_second = bandwidth_mib_s * mib;
}

ControlledStore::~ControlledStore() = default;

std::uint64_t ControlledStore::size() const {
    return impl_->source->size();
}

FetchResult ControlledStore::read(std::uint64_t offset, std::size_t length) {
    check_range(size(), offset, length);
    if (length == 0) {
        return {};
    }
    Clock::duration transfer{};
    if (impl_->bytes_per_second > 0) {
        const auto ticks = static_cast<long double>(length) / impl_->bytes_per_second *
                           Clock::period::den / Clock::period::num;
        if (!std::isfinite(ticks) ||
            ticks > static_cast<long double>(Clock::duration::max().count())) {
            throw std::invalid_argument("modeled transfer duration exceeds the clock range");
        }
        transfer = Clock::duration(static_cast<Clock::rep>(ticks));
    }
    std::this_thread::sleep_for(impl_->latency);
    std::chrono::nanoseconds queue{};
    if (impl_->bytes_per_second > 0) {
        Clock::time_point end;
        {
            std::lock_guard lock(impl_->mutex);
            const auto now = Clock::now();
            const auto begin = std::max(now, impl_->next_transfer);
            if (transfer > Clock::time_point::max() - begin) {
                throw std::invalid_argument("modeled transfer finish exceeds the clock range");
            }
            queue = begin - now;
            end = begin + transfer;
            impl_->next_transfer = end;
        }
        std::this_thread::sleep_until(end);
    }
    auto result = impl_->source->read(offset, length);
    result.backend_queue += queue;
    return result;
}

} // namespace aio
