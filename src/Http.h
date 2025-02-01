#pragma once

#include <asio.hpp>
#include <asio/ssl/stream.hpp>

#include <sese/net/http/HttpServletContext.h>
#include <sese/net/http/Range.h>
#include <sese/net/http/Controller.h>
#include <sese/io/File.h>
#include <sese/io/ByteBuilder.h>
#include <sese/util/StopWatch.h>
#include <sese/service/Service.h>
#include <sese/security/SSLContext.h>


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
    size_t timeout = 5;
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

    virtual asio::awaitable<size_t> asyncRead(void *buffer, size_t size) = 0;

    virtual asio::awaitable<size_t> asyncWrite(const void *buffer, size_t size) = 0;

    virtual void setTimeout() = 0;

    HttpServiceImpl *service;
    bool is0x0a = false;
    bool recv_status = false;
    sese::io::ByteBuilder builder;
    asio::steady_timer timer;

    asio::awaitable<void> handle();

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

    void setTimeout() override;

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

    void setTimeout() override;

    Stream stream;
};

struct HttpServiceImpl final {
    void handleFilter(Handleable *handleable);

    void handleRequest(Handleable *handleable);

private:
    asio::io_context io_context;
    std::optional<asio::ssl::context> ssl_context;
    // asio::ip::tcp::acceptor acceptor;
    asio::error_code error;

    static constexpr unsigned char ALPN_PROTOS[] = "\x2h2\x8http/1.1";

    static int alpnCallback(
        SSL *ssl,
        const uint8_t **out,
        uint8_t *out_length,
        const uint8_t *in,
        uint32_t in_length,
        void *data);

    std::string serv_name;
    size_t timeout;
    // todo to ref
    MountPointMap mount_points;
    ServletMap servlets;
    FilterMap filters;
    FilterCallback tail_filter;
};
