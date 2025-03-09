#include "Http.h"

#include <sese/Log.h>

int HttpServiceImpl::alpnCallback(
    SSL *ssl,
    const uint8_t **out,
    uint8_t *out_length,
    const uint8_t *in,
    uint32_t in_length,
    void *data) {
    if (SSL_select_next_proto(
            const_cast<unsigned char **>(out),
            out_length,
            ALPN_PROTOS,
            sizeof(ALPN_PROTOS),
            in,
            in_length
        ) != OPENSSL_NPN_NEGOTIATED) {
        *out = nullptr;
        *out_length = 0;
        return SSL_TLSEXT_ERR_NOACK;
    }
    return SSL_TLSEXT_ERR_OK;
}

HttpConnection::HttpConnection(
    HttpServiceImpl *service,
    asio::io_context &io_context,
    const sese::net::IPAddress::Ptr &addr,
    size_t timeout)
    : service(service), timer(io_context), address(addr) {
    this->timeout = timeout;
    // SESE_INFO("new connection");
}

HttpConnection::~HttpConnection() {
    // SESE_INFO("connection close");
}

HttpConnectionImpl::HttpConnectionImpl(
    HttpServiceImpl *service,
    asio::io_context &io_context,
    const sese::net::IPAddress::Ptr &addr,
    size_t timeout,
    Socket socket) : HttpConnection(service, io_context, addr, timeout), socket(std::move(socket)) {
}

asio::awaitable<size_t> HttpConnectionImpl::asyncRead(void *buffer, size_t size) {
    co_return co_await this->socket.async_read_some(asio::buffer(buffer, size), asio::use_awaitable);
}

asio::awaitable<size_t> HttpConnectionImpl::asyncWrite(const void *buffer, size_t size) {
    auto block_size = size;
    auto p = static_cast<const char *>(buffer);
    while (block_size) {
        auto wrote = co_await this->socket.async_write_some(asio::buffer(p, block_size), asio::use_awaitable);
        p += wrote;
        block_size -= wrote;
    }
    co_return size;
}

void HttpConnectionImpl::onTimeout() {
    socket.cancel();
}

HttpsConnectionImpl::HttpsConnectionImpl(
    HttpServiceImpl *service,
    asio::io_context &io_context,
    const sese::net::IPAddress::Ptr &addr,
    size_t timeout,
    Stream stream) : HttpConnection(service, io_context, addr, timeout), stream(std::move(stream)) {
}

asio::awaitable<size_t> HttpsConnectionImpl::asyncRead(void *buffer, size_t size) {
    co_return co_await this->stream.async_read_some(asio::buffer(buffer, size), asio::use_awaitable);
}

asio::awaitable<size_t> HttpsConnectionImpl::asyncWrite(const void *buffer, size_t size) {
    auto block_size = size;
    auto p = static_cast<const char *>(buffer);
    while (block_size) {
        auto wrote = co_await this->stream.async_write_some(asio::buffer(buffer, size), asio::use_awaitable);
        p += wrote;
        block_size -= wrote;
    }
    co_return size;
}

void HttpsConnectionImpl::onTimeout() {
    stream.lowest_layer().cancel();
}

HttpConnectionEx::HttpConnectionEx(
    HttpServiceImpl *service,
    asio::io_context &io_context,
    const sese::net::IPAddress::Ptr &addr,
    size_t timeout
) : service(service), address(addr), timer(io_context), timeout(timeout) {
}

HttpConnectionExImpl::HttpConnectionExImpl(
    HttpServiceImpl *service,
    asio::io_context &io_context,
    const sese::net::IPAddress::Ptr &addr,
    size_t timeout,
    Socket socket
) : HttpConnectionEx(service, io_context, addr, timeout), socket(std::move(socket)) {
}

asio::awaitable<size_t> HttpConnectionExImpl::asyncRead(void *buffer, size_t size) {
    co_return co_await this->socket.async_read_some(asio::buffer(buffer, size), asio::use_awaitable);
}

asio::awaitable<size_t> HttpConnectionExImpl::asyncWrite(const void *buffer, size_t size) {
    auto block_size = size;
    auto p = static_cast<const char *>(buffer);
    while (block_size) {
        auto wrote = co_await this->socket.async_write_some(asio::buffer(p, block_size), asio::use_awaitable);
        p += wrote;
        block_size -= wrote;
    }
    co_return size;
}

void HttpConnectionExImpl::onTimeout() {
    socket.cancel();
}

HttpsConnectionExImpl::HttpsConnectionExImpl(
    HttpServiceImpl *service,
    asio::io_context &io_context,
    const sese::net::IPAddress::Ptr &addr,
    size_t timeout,
    Stream stream) : HttpConnectionEx(service, io_context, addr, timeout), stream(std::move(stream)) {
}

asio::awaitable<size_t> HttpsConnectionExImpl::asyncRead(void *buffer, size_t size) {
    co_return co_await this->stream.async_read_some(asio::buffer(buffer, size), asio::use_awaitable);
}

asio::awaitable<size_t> HttpsConnectionExImpl::asyncWrite(const void *buffer, size_t size) {
    auto block_size = size;
    auto p = static_cast<const char *>(buffer);
    while (block_size) {
        auto wrote = co_await this->stream.async_write_some(asio::buffer(buffer, size), asio::use_awaitable);
        p += wrote;
        block_size -= wrote;
    }
    co_return size;
}

void HttpsConnectionExImpl::onTimeout() {
    stream.lowest_layer().cancel();
}
