#include "Http.h"
#include "sese/io/InputBufferWrapper.h"

asio::awaitable<void> HttpConnectionEx::handle() {
    using namespace sese::net::http;
    if (!co_await readMagic()) {
        co_return;
    }
    // todo writeSettingsFrame
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
    constexpr auto SETTINGS_FRAME_BUFFER_SIZE = 32 * 6;
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
    char raw_buffer[SETTINGS_FRAME_BUFFER_SIZE];
    if (info.length > SETTINGS_FRAME_BUFFER_SIZE ||
        info.length != co_await asyncRead(raw_buffer, info.length)) {
        co_return GOAWAY_FRAME_SIZE_ERROR;
    }

    char value_buffer[6];
    auto ident = reinterpret_cast<uint16_t *>(&value_buffer[0]);
    auto value = reinterpret_cast<uint32_t *>(&value_buffer[2]);
    auto input = sese::io::InputBufferWrapper(raw_buffer, info.length);

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
    pre_vector.push_back(std::move(frame));
}

void HttpConnectionEx::postAckFrame() {
    auto frame = std::make_unique<sese::net::http::Http2Frame>(0);
    frame->type = sese::net::http::FRAME_TYPE_SETTINGS;
    frame->flags = sese::net::http::SETTINGS_FLAGS_ACK;
    frame->buildFrameHeader();
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
    return result;
}
