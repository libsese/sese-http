#include "Http.h"

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
    : service(service), timer(io_context, asio::chrono::seconds{timeout}) {
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
    // co_return co_await this->socket.async_write_some(asio::buffer(buffer, size), asio::use_awaitable);
    auto block_size = size;
    auto p = static_cast<const char *>(buffer);
    while (block_size) {
        auto wrote = co_await this->socket.async_write_some(asio::buffer(p, block_size), asio::use_awaitable);
        p += wrote;
        block_size -= wrote;
    }
    co_return size;
}

void HttpConnectionImpl::setTimeout() {
    timer.async_wait([&](const asio::error_code &code) {
        if (code == asio::error::operation_aborted) {
            // cancel
        } else {
            // timeout
            socket.cancel();
        }
    });
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
    // co_return co_await this->stream.async_write_some(asio::buffer(buffer, size), asio::use_awaitable);
    auto block_size = size;
    auto p = static_cast<const char *>(buffer);
    while (block_size) {
        auto wrote = co_await this->stream.async_write_some(asio::buffer(buffer, size), asio::use_awaitable);
        p += wrote;
        block_size -= wrote;
    }
    co_return size;
}

void HttpsConnectionImpl::setTimeout() {
    timer.async_wait([&](const asio::error_code &code) {
        if (code == asio::error::operation_aborted) {
            // cancel
        } else {
            // timeout
            stream.lowest_layer().cancel();
        }
    });
}
