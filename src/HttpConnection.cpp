#include "HttpServiceImpl.h"

#include <sese/net/http/HttpUtil.h>

HttpConnection::HttpConnection(const std::shared_ptr<HttpServiceImpl> &service, asio::io_context &context, const sese::net::IPAddress::Ptr &addr)
    : Handleable(), timer(context, asio::chrono::seconds{service->getKeepalive()}),
      expect_length(0),
      real_length(0),
      service(service) {
    remote_address = addr;
}

void HttpConnection::readHeader() {
    node = std::make_unique<sese::IOBufNode>(MTU_VALUE);
    asyncReadSome(asio::buffer(node->buffer, MTU_VALUE), [conn = getPtr()](const asio::error_code &error, std::size_t bytes_transferred) {
        if (conn->keepalive) {
            conn->keepalive = false;
            conn->timer.cancel();
        }
        if (error) {
            // There was an error and it should be disconnected
            conn->disponse();
            conn->node = nullptr;
            return;
        }
        conn->node->size = bytes_transferred;
        bool recv_status = false;
        bool parse_status = false;
        for (int i = 0; i < bytes_transferred; ++i) {
            if (conn->is0x0a && static_cast<char *>(conn->node->buffer)[i] == '\r') {
                conn->is0x0a = false;
                recv_status = true;
                conn->io_buffer.push(std::move(conn->node));
                parse_status = sese::net::http::HttpUtil::recvRequest(&conn->io_buffer, &conn->request);
                break;
            }
            conn->is0x0a = static_cast<char *>(conn->node->buffer)[i] == '\n';
        }
        if (!recv_status) {
            // Receive incomplete, save existing results and continue receiving
            // SESE_WARN("read again");
            conn->io_buffer.push(std::move(conn->node));
            conn->readHeader();
            return;
        }
        if (!parse_status) {
            // Parsing failed and should be disconnected
            // SESE_ERROR("Parsing failed");
            conn->disponse();
            return;
        }

        auto service = conn->service.lock();
        service->handleFilter(conn);

        conn->expect_length = sese::toInteger(conn->request.get("content-length", "0"));
        conn->real_length = conn->io_buffer.getReadableSize();
        if (conn->real_length && conn->conn_type != ConnType::FILTER) {
            // Part of the body
            streamMove(&conn->request.getBody(), &conn->io_buffer, conn->real_length);
        }
        conn->io_buffer.clear();
        conn->node = nullptr;
        if (conn->expect_length != conn->real_length) {
            conn->readBody();
        } else {
            conn->handleRequest();
        }
    });
}

void HttpConnection::readBody() {
    node = std::make_unique<sese::IOBufNode>(MTU_VALUE);
    asyncReadSome(asio::buffer(node->buffer, MTU_VALUE), [conn = getPtr()](const asio::error_code &error, std::size_t bytes_transferred) {
        if (error) {
            // There was an error and it should be disconnected
            conn->disponse();
            return;
        }
        conn->node->size = bytes_transferred;
        conn->real_length += conn->node->size;
        auto node_size = conn->node->size;
        conn->io_buffer.push(std::move(conn->node));
        if (conn->conn_type != ConnType::FILTER) {
            streamMove(&conn->request.getBody(), &conn->io_buffer, node_size);
        }
        if (conn->real_length >= conn->expect_length) {
            // Theoretically, real_length can't be greater than expect_length,
            // and here is a precaution
            conn->io_buffer.clear();
            conn->node = nullptr;
            conn->handleRequest();
        } else {
            conn->readBody();
        }
    });
}

void HttpConnection::handleRequest() {
    auto serv = service.lock();
    assert(serv);
    serv->handleRequest(shared_from_this());
    sese::net::http::HttpUtil::sendResponse(&this->dynamic_buffer, &this->response);
    this->real_length = 0;
    this->expect_length = this->dynamic_buffer.getReadableSize();
    this->writeHeader();
}

void HttpConnection::writeHeader() {
    auto l = std::min<size_t>(this->expect_length - this->real_length, MTU_VALUE);
    l = this->dynamic_buffer.read(this->send_buffer, l);
    this->real_length += l;
    this->writeBlock(this->send_buffer, l, [conn = getPtr()](const asio::error_code &error) {
        if (error) {
            // There was an error and it should be disconnected
            conn->disponse();
            return;
        }
        if (conn->expect_length > conn->real_length) {
            conn->writeHeader();
        } else {
            conn->dynamic_buffer.freeCapacity();
            conn->expect_length = 0;
            conn->real_length = 0;
            if (conn->ranges.size() == 1) {
                // Single range file
                conn->expect_length = conn->range_iterator->len;
                if (conn->file->setSeek(static_cast<int64_t>(conn->range_iterator->begin), sese::io::Seek::BEGIN)) {
                    conn->disponse();
                    return;
                }
                conn->writeSingleRange();
            } else if (conn->ranges.size() > 1) {
                // Multi range file
                conn->writeRanges();
            } else if ((conn->expect_length = conn->response.getBody().getReadableSize())) {
                // body resp
                conn->writeBody();
            } else {
                // keepalive
                conn->checkKeepalive();
            }
        }
    });
}

void HttpConnection::writeBody() {
    auto l = std::min<size_t>(this->expect_length - this->real_length, MTU_VALUE);
    l = response.getBody().read(this->send_buffer, l);
    this->real_length += l;
    this->writeBlock(this->send_buffer, l, [conn = getPtr()](const asio::error_code &error) {
        if (error) {
            // There was an error and it should be disconnected
            conn->disponse();
            return;
        }
        if (conn->expect_length > conn->real_length) {
            conn->writeBody();
        } else {
            // keepalive
            conn->checkKeepalive();
        }
    });
}

void HttpConnection::writeSingleRange() {
    auto l = std::min<size_t>(this->expect_length - this->real_length, MTU_VALUE);
    this->real_length += l;
    l = this->file->read(this->send_buffer, l);
    this->writeBlock(this->send_buffer, l, [conn = shared_from_this()](const asio::error_code &error) {
        if (error) {
            conn->disponse();
            return;
        }
        if (conn->expect_length > conn->real_length) {
            conn->writeSingleRange();
        } else {
            // keepalive
            conn->checkKeepalive();
        }
    });
}

void HttpConnection::writeRanges() {
    if (this->real_length >= this->expect_length) {
        if (this->range_iterator == this->ranges.begin()) {
            // The first range
            auto subheader = std::string("--") + HTTPD_BOUNDARY + "\r\n" +
                             "content-type: " + this->content_type + "\r\n" +
                             "content-range: " + this->range_iterator->toString(this->filesize) + "\r\n\r\n";
            assert(subheader.length() <= MTU_VALUE);
            std::memcpy(this->send_buffer, subheader.data(), subheader.length());
            this->writeBlock(this->send_buffer, subheader.length(), [conn = shared_from_this()](const asio::error_code &error) {
                if (error) {
                    conn->disponse();
                    return;
                }
                if (conn->file->setSeek(static_cast<int64_t>(conn->range_iterator->begin), sese::io::Seek::BEGIN)) {
                    conn->disponse();
                    return;
                }
                conn->expect_length = conn->range_iterator->len;
                conn->real_length = 0;
                conn->writeRanges();
                conn->range_iterator += 1;
            });
            return;
        }
        if (this->range_iterator == this->ranges.end()) {
            // The last range
            auto subheader = std::string("\r\n--") + HTTPD_BOUNDARY + "--\r\n";
            std::memcpy(this->send_buffer, subheader.data(), subheader.length());
            assert(subheader.length() <= MTU_VALUE);
            this->writeBlock(this->send_buffer, subheader.length(), [conn = shared_from_this()](const asio::error_code &error) {
                if (error) {
                    conn->disponse();
                    return;
                }
                conn->checkKeepalive();
            });
        } else {
            // The ranges in between
            auto subheader = std::string("\r\n--") + HTTPD_BOUNDARY + "\r\n" +
                             "content-type: " + this->content_type + "\r\n" +
                             "content-range: " + this->range_iterator->toString(this->filesize) + "\r\n\r\n";
            assert(subheader.length() <= MTU_VALUE);
            std::memcpy(this->send_buffer, subheader.data(), subheader.length());
            this->writeBlock(this->send_buffer, subheader.length(), [conn = shared_from_this()](const asio::error_code &error) {
                if (error) {
                    conn->disponse();
                    return;
                }
                if (conn->file->setSeek(static_cast<int64_t>(conn->range_iterator->begin), sese::io::Seek::BEGIN)) {
                    conn->disponse();
                    return;
                }
                conn->expect_length = conn->range_iterator->len;
                conn->real_length = 0;
                conn->writeRanges();
                conn->range_iterator += 1;
            });
        }
    } else {
        auto l = std::min<size_t>(this->expect_length - this->real_length, MTU_VALUE);
        this->real_length += l;
        l = this->file->read(this->send_buffer, l);
        this->writeBlock(this->send_buffer, l, [conn = shared_from_this()](const asio::error_code &error) {
            if (error) {
                conn->disponse();
                return;
            }
            conn->writeRanges();
        });
    }
}

void HttpConnection::disponse() {
    auto serv = service.lock();
    assert(serv);
    serv->connections.erase(shared_from_this());
}

void HttpConnection::reset() {
    conn_type = ConnType::NONE;

    expect_length = 0;
    real_length = 0;
    ranges.clear();

    request.clear();
    request.queryArgsClear();
    request.getBody().freeCapacity();
    if (auto cookies = request.getCookies())
        cookies->clear();

    response.setCode(200);
    response.clear();
    response.getBody().freeCapacity();
    if (auto cookies = response.getCookies()) cookies->clear();
}
