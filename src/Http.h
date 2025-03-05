#pragma once

#include <asio.hpp>
#include <asio/ssl/stream.hpp>
#include <optional>

#include <sese/net/http/HttpServletContext.h>
#include <sese/net/http/Range.h>
#include <sese/net/http/Controller.h>
#include <sese/net/http/DynamicTable.h>
#include <sese/net/http/Http2Frame.h>
#include <sese/io/File.h>
#include <sese/io/ByteBuilder.h>
#include <sese/util/StopWatch.h>
#include <sese/service/Service.h>
#include <sese/security/SSLContext.h>
#include <sese/thread/Thread.h>


using SSLContextPtr = std::unique_ptr<sese::security::SSLContext>;
using FilterCallback = std::function<bool(sese::net::http::Request &, sese::net::http::Response &)>;
using ConnectionCallback = std::function<bool(sese::net::IPAddress::Ptr &)>;
using FilterMap = std::unordered_map<std::string, FilterCallback>;
using MountPointMap = std::unordered_map<std::string, std::string>;
using ServletMap = std::unordered_map<std::string, sese::net::http::Servlet>;

struct Handleable {
    enum class ConnType {
        FILTER,
        FILE_DOWNLOAD,
        CONTROLLER,
        NONE
    };

    virtual ~Handleable() = default;

    ConnType conn_type = ConnType::NONE;
    sese::net::http::Request request;
    sese::net::http::Response response;
    std::string content_type = "application/x-";
    sese::io::File::Ptr file;
    size_t filesize = 0;
    std::vector<sese::net::http::Range> ranges;
    sese::net::IPAddress::Ptr remote_address{};
    bool keepalive = false;
    size_t timeout;
    sese::StopWatch stopwatch;
};

struct HttpServiceImpl;

struct HttpConnection : Handleable {
    HttpConnection(
        HttpServiceImpl *service,
        asio::io_context &io_context,
        const sese::net::IPAddress::Ptr &addr,
        size_t timeout
    );

    ~HttpConnection() override;

    virtual asio::awaitable<size_t> asyncRead(void *buffer, size_t size) = 0;

    virtual asio::awaitable<size_t> asyncWrite(const void *buffer, size_t size) = 0;

    virtual void onTimeout() = 0;

    HttpServiceImpl *service;
    bool is0x0a = false;
    bool recv_status = false;
    sese::io::ByteBuilder builder;
    asio::steady_timer timer;
    sese::net::IPAddress::Ptr address;

    asio::awaitable<void> handle();

    asio::awaitable<void> writeHeader();

    asio::awaitable<void> writeBody();

    asio::awaitable<void> writeSingleRange();

    asio::awaitable<void> writeRanges();
};

struct HttpConnectionImpl final : HttpConnection {
    using Socket = asio::ip::tcp::socket;

    HttpConnectionImpl(
        HttpServiceImpl *service,
        asio::io_context &io_context,
        const sese::net::IPAddress::Ptr &addr,
        size_t timeout,
        Socket socket
    );

    asio::awaitable<size_t> asyncRead(void *buffer, size_t size) override;

    asio::awaitable<size_t> asyncWrite(const void *buffer, size_t size) override;

    void onTimeout() override;

    Socket socket;
};

struct HttpsConnectionImpl final : HttpConnection {
    using Stream = asio::ssl::stream<asio::ip::tcp::socket>;

    HttpsConnectionImpl(
        HttpServiceImpl *service,
        asio::io_context &io_context,
        const sese::net::IPAddress::Ptr &addr,
        size_t timeout,
        Stream stream
    );

    asio::awaitable<size_t> asyncRead(void *buffer, size_t size) override;

    asio::awaitable<size_t> asyncWrite(const void *buffer, size_t size) override;

    void onTimeout() override;

    Stream stream;
};

struct HttpStream final : Handleable {
    using Ptr = std::shared_ptr<HttpStream>;

    HttpStream(uint32_t id, uint32_t write_window_size, const sese::net::IPAddress::Ptr &addr);

    uint32_t id;
    uint32_t endpoint_window_size;
    uint32_t window_size = 0;
    uint32_t continue_type = 0;
    bool end_headers = false;
    bool end_stream = false;
    bool do_response = false;

    sese::io::ByteBuilder builder;
};

struct HttpConnectionEx {
    HttpConnectionEx(
        HttpServiceImpl *service,
        asio::io_context &io_context,
        const sese::net::IPAddress::Ptr &addr,
        size_t timeout
    );

    virtual ~HttpConnectionEx() = default;

    virtual asio::awaitable<size_t> asyncWrite(const void *buffer, size_t size) = 0;

    virtual asio::awaitable<size_t> asyncRead(void *buffer, size_t size) = 0;

    virtual void onTimeout() = 0;

    asio::awaitable<void> handle();

    /// \brief handle settings frame
    /// \retval UIN8_MAX this frame is ack frame
    /// \retval 0 all be ok
    /// \retval else error code
    asio::awaitable<uint8_t> handleSettingsFrame();

    asio::awaitable<bool> handleWindowUpdate();

    void handleRstStreamFrame();

    void handleGoawayFrame();

    void handlePingFrame();

    void handlePriorityFrame();

    void handleHeadersFrame();

    void handleDataFrame();

    void triggerWrite();

    asio::awaitable<bool> readMagic();

    asio::awaitable<bool> readFrameHeader();

    /// write goaway frame
    /// \param latest_stream_id The last stream ID
    /// \param flags Flags
    /// \param error_code Error code
    /// \param msg Error message, can be nullptr
    /// \param immediately Immediately send, else just push to the queue(default)
    /// \return Success or not
    asio::awaitable<bool> postGoawayFrame(
        uint32_t latest_stream_id,
        uint8_t flags,
        uint32_t error_code,
        const std::optional<std::string> &msg = nullptr,
        bool immediately = false
    );

    asio::awaitable<bool> postRstStreamFrame(
        uint32_t stream_id,
        uint8_t flags,
        uint32_t error_code,
        bool immediately = false
    );

    void postSettingsFrame();

    void postAckFrame();

    void postWindowUpdateFrame(uint32_t stream_id, uint8_t flags, uint32_t window_size);

    bool postHeadersFrame(const HttpStream::Ptr &stream, bool verify_end_stream);

    sese::net::http::Http2FrameInfo info;
    HttpServiceImpl *service;
    sese::net::IPAddress::Ptr address;
    bool keepalive = false;
    bool expect_ack = false;
    asio::steady_timer timer;
    size_t timeout;

    uint32_t accept_stream_count = 0;
    uint32_t latest_stream_ident = 0;

    // The maximum local frame size
    static constexpr uint32_t MAX_FRAME_SIZE = 16384;
    // Local initial window value
    static constexpr uint32_t INIT_WINDOW_SIZE = 65535;
    // Default dynamic table size
    static constexpr uint32_t HEADER_TABLE_SIZE = 8192;
    // The size of a single connection concurrency
    static constexpr uint32_t MAX_CONCURRENT_STREAMS = 16;

    uint32_t header_table_size = 4096;
    uint32_t enable_push = 0;
    uint32_t max_concurrent_stream = 0;
    // The value of the initial window on the peer
    uint32_t endpoint_init_window_size = 65535;
    // Write to the peer window size
    uint32_t endpoint_window_size = 65535;
    // The size of the local read window
    uint32_t window_size = 65535;
    // The maximum size of the peer frame
    uint32_t endpoint_max_frame_size = 16384;
    // The frame size used
    uint32_t max_frame_size = 16384;
    uint32_t max_header_list_size = 0;
    sese::net::http::DynamicTable req_dynamic_table;
    sese::net::http::DynamicTable resp_dynamic_table;
    std::map<uint32_t, HttpStream::Ptr> streams;
    std::set<uint32_t> closed_streams;

    char buffer[MAX_FRAME_SIZE];
    /// Send queues
    std::vector<sese::net::http::Http2Frame::Ptr> pre_vector;
    std::vector<sese::net::http::Http2Frame::Ptr> vector;

    /// Close stream
    /// @param id Stream ID
    // void close(uint32_t id);
};

struct HttpConnectionExImpl final : HttpConnectionEx {
    using Socket = asio::ip::tcp::socket;

    HttpConnectionExImpl(
        HttpServiceImpl *service,
        asio::io_context &io_context,
        const sese::net::IPAddress::Ptr &addr,
        size_t timeout,
        Socket socket
    );

    asio::awaitable<size_t> asyncWrite(const void *buffer, size_t size) override;

    asio::awaitable<size_t> asyncRead(void *buffer, size_t size) override;

    void onTimeout() override;

    Socket socket;
};

struct HttpsConnectionExImpl final : HttpConnectionEx {
    using Stream = asio::ssl::stream<asio::ip::tcp::socket>;

    HttpsConnectionExImpl(
        HttpServiceImpl *service,
        asio::io_context &io_context,
        const sese::net::IPAddress::Ptr &addr,
        size_t timeout,
        Stream stream
    );

    asio::awaitable<size_t> asyncWrite(const void *buffer, size_t size) override;

    asio::awaitable<size_t> asyncRead(void *buffer, size_t size) override;

    void onTimeout() override;

    Stream stream;
};

struct HttpServiceImpl final : sese::service::Service {
    HttpServiceImpl(
        const sese::net::IPAddress::Ptr &address,
        SSLContextPtr ssl_context,
        std::string serv_name,
        size_t timeout,
        size_t io_threads,
        ConnectionCallback &connection_callback,
        MountPointMap &mount_points,
        ServletMap &servlets,
        FilterMap &filters,
        FilterCallback &tail_filter
    );

    bool startup() override;

    bool shutdown() override;

    int getLastError() override;

    std::string getLastErrorMessage() override;

    void handleFilter(Handleable *handleable);

    void handleRequest(Handleable *handleable);

    asio::awaitable<void> handleAccept();

    asio::awaitable<void> handleSslAccept();

private:
    asio::io_context io_context;
    asio::ip::tcp::endpoint endpoint;
    std::optional<asio::ssl::context> ssl_context;
    asio::ip::tcp::acceptor acceptor;
    asio::error_code error;

    static constexpr unsigned char ALPN_PROTOS[] = "\x2h2\x8http/1.1";

    static int alpnCallback(
        SSL *ssl,
        const uint8_t **out,
        uint8_t *out_length,
        const uint8_t *in,
        uint32_t in_length,
        void *data
    );

    sese::net::IPAddress::Ptr address;
    std::string serv_name;
    size_t timeout;
    std::vector<sese::Thread> threads;
    ConnectionCallback &connection_callback;
    MountPointMap &mount_points;
    ServletMap &servlets;
    FilterMap &filters;
    FilterCallback &tail_filter;
};
