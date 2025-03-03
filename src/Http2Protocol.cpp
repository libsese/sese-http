#include "Http.h"
#include "sese/io/InputBufferWrapper.h"
#include "sese/net/http/HPackUtil.h"
#include "sese/net/http/HttpConverter.h"

asio::awaitable<void> HttpConnectionEx::handle() {
    using namespace sese::net::http;
    if (!co_await readMagic()) {
        co_return;
    }
    // todo postSettingsFrame
    while (true) {
        if (!co_await readFrameHeader()) {
            co_return;
        }
        auto iterator = streams.find(info.ident);
        // CONTINUATION frames are not continuous
        // Judgment pre-sequence frame 2
        if (iterator != streams.end()) {
            if (info.type != FRAME_TYPE_CONTINUATION &&
                iterator->second->continue_type == FRAME_TYPE_CONTINUATION &&
                iterator->second->end_headers == false) {
                co_await postGoawayFrame(info.ident, 0, GOAWAY_PROTOCOL_ERROR, nullptr);
                co_return;
            }
        }
        switch (info.type) {
            case FRAME_TYPE_SETTINGS: {
                break;
            }
            case FRAME_TYPE_WINDOW_UPDATE: {
                break;
            }
            case FRAME_TYPE_GOAWAY: {
                break;
            }
            case FRAME_TYPE_CONTINUATION: {
                break;
            }
            case FRAME_TYPE_HEADERS: {
                break;
            }
            case FRAME_TYPE_DATA: {
                break;
            }
            case FRAME_TYPE_PRIORITY: {
                break;
            }
            case FRAME_TYPE_RST_STREAM: {
                break;
            }
            case FRAME_TYPE_PING: {
                break;
            }
            default: {
                break;
            }
        }
    }
}

asio::awaitable<uint8_t> HttpConnectionEx::handleSettingsFrame() {
    using namespace sese::net::http;
    if (info.ident != 0) {
        co_return GOAWAY_PROTOCOL_ERROR;
    }
    if (info.flags & SETTINGS_FLAGS_ACK) {
        if (info.length) {
            co_return GOAWAY_FRAME_SIZE_ERROR;
        }
        expect_ack = false;
        co_return UINT8_MAX;
    }
    if (info.length % 6) {
        co_return GOAWAY_FRAME_SIZE_ERROR;
    }

    char value_buffer[6];
    auto ident = reinterpret_cast<uint16_t *>(&value_buffer[0]);
    auto value = reinterpret_cast<uint32_t *>(&value_buffer[2]);
    auto input = sese::io::InputBufferWrapper(buffer, info.length);

    while (input.read(value_buffer, 6) == 6) {
        *ident = FromBigEndian16(*ident);
        *value = FromBigEndian32(*value);

        switch (*ident) {
            case SETTINGS_HEADER_TABLE_SIZE:
                this->req_dynamic_table.resize(*value);
                this->header_table_size = *value;
                break;
            case SETTINGS_MAX_CONCURRENT_STREAMS:
                this->max_concurrent_stream = *value == 0 ? 100 : *value;
                break;
            case SETTINGS_MAX_FRAME_SIZE:
                if (*value > 16777215 || *value < 16384) {
                    co_return GOAWAY_PROTOCOL_ERROR;
                }
                this->endpoint_max_frame_size = *value;
                this->max_frame_size = std::min(this->endpoint_max_frame_size, MAX_FRAME_SIZE);
                break;
            case SETTINGS_ENABLE_PUSH:
                if (*value <= 1) {
                    this->enable_push = *value;
                } else {
                    co_return GOAWAY_PROTOCOL_ERROR;
                }
                break;
            case SETTINGS_MAX_HEADER_LIST_SIZE:
                this->max_header_list_size = *value;
                req_dynamic_table.resize(max_header_list_size);
                resp_dynamic_table.resize(max_header_list_size);
                break;
            case SETTINGS_INITIAL_WINDOW_SIZE:
                if (accept_stream_count) {
                    co_return GOAWAY_FLOW_CONTROL_ERROR;
                }
                if (*value > 2147483647) {
                    co_return GOAWAY_FLOW_CONTROL_ERROR;
                }
                this->endpoint_init_window_size = *value;
                break;
            default:
                break;
        }
    }
    co_return 0;
}

asio::awaitable<bool> HttpConnectionEx::handleWindowUpdate() {
    using namespace sese::net::http;
    auto data = reinterpret_cast<uint32_t *>(buffer);
    if (info.length != 4) {
        postGoawayFrame(info.ident, 0, GOAWAY_FRAME_SIZE_ERROR, nullptr);
        co_return false;
    }
    auto i = FromBigEndian32(*data);
    if (i == 0) {
        postGoawayFrame(info.ident, 0, GOAWAY_PROTOCOL_ERROR, nullptr);
        co_return false;
    }

    if (info.ident == 0) {
        if (sese::isAdditionOverflow<
            int32_t>(static_cast<int32_t>(endpoint_init_window_size), static_cast<int32_t>(i))) {
            postGoawayFrame(info.ident, 0, GOAWAY_FLOW_CONTROL_ERROR, nullptr);
            co_return false;
        }
        endpoint_window_size += i;
    } else {
        auto iterator = streams.find(info.ident);
        if (iterator != streams.end()) {
            auto stream = iterator->second;
            stream->continue_type = info.type;
            if (sese::isAdditionOverflow<int32_t>(static_cast<int32_t>(stream->endpoint_window_size),
                                                  static_cast<int32_t>(i))) {
                postRstStreamFrame(info.ident, 0, GOAWAY_FLOW_CONTROL_ERROR);
                co_return false;
            }
            stream->endpoint_window_size += i;
        }
    }
    co_return true;
}

void HttpConnectionEx::handleRstStreamFrame() {
    using namespace sese::net::http;
    if (info.ident == 0) {
        postGoawayFrame(0, 0, GOAWAY_PROTOCOL_ERROR, "");
        return;
    }
    if (info.length != 4) {
        postGoawayFrame(0, 0, GOAWAY_FRAME_SIZE_ERROR, "");
        return;
    }

    if (closed_streams.contains(info.ident)) {
        // postGoawayFrame(info.ident, 0, GOAWAY_STREAM_CLOSED, "");
        readFrameHeader();
    }

    auto iterator = streams.find(info.ident);
    if (iterator == streams.end()) {
        postGoawayFrame(info.ident, 0, GOAWAY_PROTOCOL_ERROR, "");
        return;
    }
    auto stream = iterator->second;
    // todo close(stream->id);

    uint32_t code;
    memcpy(buffer, &code, 4);
    code = FromBigEndian32(code);
}

void HttpConnectionEx::handleGoawayFrame() {
    using namespace sese::net::http;
    if (info.ident != 0) {
        postGoawayFrame(0, 0, GOAWAY_PROTOCOL_ERROR);
        return;
    }
    auto iterator = streams.find(info.ident);
    if (iterator != streams.end()) {
        iterator->second->continue_type = info.type;
    }
    uint32_t latest_stream;
    memcpy(&latest_stream, buffer, sizeof(latest_stream));
    latest_stream = FromBigEndian32(latest_stream);
    uint32_t error_code;
    memcpy(&error_code, buffer + 4, sizeof(latest_stream));
    error_code = FromBigEndian32(error_code);
    if (info.length - 8) {
        auto msg = std::string(buffer + 8, info.length - 8);
        // SESE_WARN("FAILED: LS {} CODE {} MSG {}", latest_stream, error_code, msg);
        if (msg == "shutdown") {
            return;
        }
    } else {
        // SESE_WARN("FAILED: LS {} CODE {}", latest_stream, error_code);
    }
}

void HttpConnectionEx::handleHeadersFrame() {
    using namespace sese::net::http;
    timer.cancel();
    if (info.ident == 0 ||
        info.ident % 2 != 1) {
        postGoawayFrame(0, 0, GOAWAY_PROTOCOL_ERROR);
        return;
        }

    if (closed_streams.contains(info.ident)) {
        postGoawayFrame(info.ident, 0, GOAWAY_STREAM_CLOSED);
        return;
    }

    HttpStream::Ptr stream;
    auto iterator = streams.find(info.ident);
    if (info.type == FRAME_TYPE_HEADERS) {
        if (iterator == streams.end()) {
            if (info.ident < latest_stream_ident) {
                postGoawayFrame(0, 0, GOAWAY_PROTOCOL_ERROR);
                return;
            }
            if (streams.size() > MAX_CONCURRENT_STREAMS) {
                postGoawayFrame(0, 0, GOAWAY_PROTOCOL_ERROR, "shutdown", true);
                return;
            }
            stream = std::make_shared<HttpStream>(info.ident, endpoint_init_window_size, address);
            streams[info.ident] = stream;
            accept_stream_count += 1;
            latest_stream_ident = info.ident;
        } else {
            stream = iterator->second;
            if (stream->end_headers) {
                postGoawayFrame(info.ident, 0, GOAWAY_PROTOCOL_ERROR);
                return;
            }
        }
    } else {
        if (iterator == streams.end()) {
            postGoawayFrame(info.ident, 0, GOAWAY_PROTOCOL_ERROR);
            return;
        }
        stream = iterator->second;
    }

    uint8_t offset = 0;
    uint8_t padded = 0;
    stream->continue_type = info.type;
    if (info.flags & FRAME_FLAG_PADDED) {
        padded = buffer[0];
        offset += 1;
    }
    if (info.flags & FRAME_FLAG_PRIORITY) {
        uint32_t dependency;
        memcpy(&dependency, buffer + offset, 4);
        dependency = FromBigEndian32(dependency);
        offset += 4;
        uint8_t priority = buffer[offset]; // NOLINT
        offset += 1;

        if (dependency == info.ident) {
            postGoawayFrame(info.ident, 0, GOAWAY_PROTOCOL_ERROR, "");
            return;
        }
    }

    if (padded > info.length) {
        postGoawayFrame(info.ident, 0, GOAWAY_PROTOCOL_ERROR, "");
        return;
    }

    stream->builder.write(buffer + offset, info.length - padded - offset);

    if (info.flags & FRAME_FLAG_END_HEADERS) {
        stream->end_headers = true;
    }
    if (info.flags & FRAME_FLAG_END_STREAM) {
        stream->end_stream = true;
    }

    if (stream->end_headers) {
        auto rt = HPackUtil::decode(&stream->builder, stream->builder.getReadableSize(), req_dynamic_table, stream->request, false, true, header_table_size);
        stream->builder.freeCapacity();
        if (rt) {
            postGoawayFrame(info.ident, 0, rt);
            return;
        }

        rt = HttpConverter::convertFromHttp2(&stream->request);
        if (!rt) {
            postGoawayFrame(info.ident, 0, GOAWAY_PROTOCOL_ERROR);
            return;
        }

        service->handleFilter(stream.get());

        if (stream->end_stream) {
            service->handleRequest(stream.get());

            HttpConverter::convert2Http2(&stream->response);
            Header header;
            HPackUtil::encode(&stream->builder, resp_dynamic_table, header, stream->response);

            triggerWrite();
        }
    }
}

void HttpConnectionEx::triggerWrite() {
    // todo co_spawn
}

asio::awaitable<bool> HttpConnectionEx::readMagic() {
    auto len = co_await asyncRead(buffer, 24);
    if (len != 24 ||
        0 != strncmp(buffer, sese::net::http::MAGIC_STRING, 24)) {
        co_return false;
    }
    co_return true;
}

asio::awaitable<bool> HttpConnectionEx::readFrameHeader() {
    using namespace sese::net::http;
    auto len = co_await asyncRead(buffer, 9);
    if (len != 9) {
        co_return false;
    }
    memset(&info, 0, sizeof(info));
    memcpy(reinterpret_cast<char *>(&info.length) + 1, buffer + 0, 3);
    memcpy(&info.type, buffer + 3, 1);
    memcpy(&info.flags, buffer + 4, 1);
    memcpy(&info.ident, buffer + 5, 4);
    info.length = FromBigEndian32(info.length);
    info.ident = FromBigEndian32<uint32_t>(info.ident);
    info.ident &= 0x7fffffff;

    if (info.length > max_frame_size) {
        co_await postGoawayFrame(info.ident, 0, GOAWAY_PROTOCOL_ERROR, "shutdown", true);
        co_return false;
    }
    len = co_await asyncRead(buffer, info.length);
    if (len != info.length) {
        co_return false;
    }
    co_return true;
}

asio::awaitable<bool> HttpConnectionEx::postGoawayFrame(
    uint32_t latest_stream_id,
    uint8_t flags,
    uint32_t error_code,
    const std::optional<std::string> &msg,
    bool immediately) {
    using namespace sese::net::http;
    auto frame_length = (msg.has_value() ? msg.value().length() : 0) + 8;
    auto frame = std::make_unique<Http2Frame>(frame_length);
    frame->type = FRAME_TYPE_GOAWAY;
    frame->flags = flags;
    frame->ident = 0;
    frame->length = static_cast<uint32_t>(frame_length);
    frame->buildFrameHeader();
    latest_stream_id = ToBigEndian32(latest_stream_id);
    error_code = ToBigEndian32(error_code);
    memcpy(frame->getFrameContentBuffer() + 0, &latest_stream_id, 4);
    memcpy(frame->getFrameContentBuffer() + 4, &error_code, 4);
    if (immediately) {
        auto len = co_await asyncWrite(frame->getFrameBuffer(), frame->getFrameLength());
        if (len != frame->getFrameLength()) {
            co_return false;
        }
        co_return true;
    }
    pre_vector.push_back(std::move(frame));
    triggerWrite();
    co_return true;
}

asio::awaitable<bool> HttpConnectionEx::postRstStreamFrame(
    uint32_t stream_id,
    uint8_t flags,
    uint32_t error_code,
    bool immediately) {
    using namespace sese::net::http;
    auto frame = std::make_unique<Http2Frame>(4);
    frame->type = FRAME_TYPE_RST_STREAM;
    frame->flags = flags;
    frame->ident = stream_id;
    frame->length = 4;
    frame->buildFrameHeader();
    error_code = ToBigEndian32(error_code);
    memcpy(frame->getFrameContentBuffer(), &error_code, 4);
    if (immediately) {
        auto len = co_await asyncWrite(frame->getFrameBuffer(), frame->getFrameLength());
        if (len != frame->getFrameLength()) {
            co_return false;
        }
        co_return true;
    }
    pre_vector.push_back(std::move(frame));
    triggerWrite();
    co_return true;
}

void HttpConnectionEx::postSettingsFrame() {
    using namespace sese::net::http;
    std::vector<std::pair<uint16_t, uint32_t>> values = {
        {SETTINGS_INITIAL_WINDOW_SIZE, INIT_WINDOW_SIZE},
        {SETTINGS_MAX_FRAME_SIZE, MAX_FRAME_SIZE},
        {SETTINGS_HEADER_TABLE_SIZE, HEADER_TABLE_SIZE},
        {SETTINGS_MAX_CONCURRENT_STREAMS, MAX_CONCURRENT_STREAMS}
    };

    auto frame = std::make_unique<Http2Frame>(values.size() * 6);
    frame->length = static_cast<uint32_t>(values.size() * 6);
    frame->type = FRAME_TYPE_SETTINGS;
    frame->flags = 0;
    frame->ident = 0;
    frame->buildFrameHeader();

    auto buffer = frame->getFrameContentBuffer();
    int pos = 0;
    for (auto [key, value]: values) {
        key = ToBigEndian16(key);
        value = ToBigEndian32(value);
        memcpy(buffer + pos, &key, sizeof(key));
        pos += sizeof(key);
        memcpy(buffer + pos, &value, sizeof(value));
        pos += sizeof(value);
    }
    expect_ack = true;
    triggerWrite();
    pre_vector.push_back(std::move(frame));
}

void HttpConnectionEx::postAckFrame() {
    auto frame = std::make_unique<sese::net::http::Http2Frame>(0);
    frame->type = sese::net::http::FRAME_TYPE_SETTINGS;
    frame->flags = sese::net::http::SETTINGS_FLAGS_ACK;
    frame->buildFrameHeader();
    triggerWrite();
    pre_vector.push_back(std::move(frame));
}

void HttpConnectionEx::postWindowUpdateFrame(
    uint32_t stream_id,
    uint8_t flags,
    uint32_t window_size) {
    auto frame = std::make_unique<sese::net::http::Http2Frame>(4);
    frame->type = sese::net::http::FRAME_TYPE_WINDOW_UPDATE;
    frame->length = 4;
    frame->ident = stream_id;
    frame->flags = flags;
    frame->buildFrameHeader();
    window_size = ToBigEndian32(window_size);
    memcpy(frame->getFrameContentBuffer(), &window_size, 4);
    pre_vector.push_back(std::move(frame));
    triggerWrite();
}

bool HttpConnectionEx::postHeadersFrame(const HttpStream::Ptr &stream, bool verify_end_stream) {
    auto result = false;
    auto frame = std::make_unique<sese::net::http::Http2Frame>(max_frame_size);
    frame->ident = stream->id;
    auto len = stream->builder.read(frame->getFrameContentBuffer(), max_frame_size);
    frame->type = sese::net::http::FRAME_TYPE_HEADERS;
    frame->length = static_cast<uint32_t>(len);
    if (stream->builder.eof()) {
        frame->flags |= sese::net::http::FRAME_FLAG_END_HEADERS;
    }
    if (verify_end_stream && stream->response.getBody().eof()) {
        frame->flags |= sese::net::http::FRAME_FLAG_END_STREAM;
        result = true;
    }
    frame->buildFrameHeader();
    pre_vector.push_back(std::move(frame));
    triggerWrite();
    return result;
}
