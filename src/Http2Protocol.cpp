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
                co_await writeGoawayFrame(info.ident, 0, GOAWAY_PROTOCOL_ERROR, nullptr);
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


asio::awaitable<bool> HttpConnectionEx::readMagic() {
    char buffer[24];
    auto len = co_await asyncRead(buffer, sizeof(buffer));
    if (len != sizeof(buffer) ||
        0 != strncmp(buffer, sese::net::http::MAGIC_STRING, sizeof(buffer))) {
        co_return false;
    }
    co_return true;
}

asio::awaitable<bool> HttpConnectionEx::readFrameHeader() {
    using namespace sese::net::http;
    char buffer[9];
    auto len = co_await asyncRead(buffer, sizeof(buffer));
    if (len != sizeof(buffer)) {
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

    if (info.length > endpoint_max_frame_size) {
        co_await writeGoawayFrame(info.ident, 0, GOAWAY_PROTOCOL_ERROR, "shutdown", true);
        co_return false;
    }
    co_return true;
}

asio::awaitable<bool> HttpConnectionEx::writeGoawayFrame(
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
