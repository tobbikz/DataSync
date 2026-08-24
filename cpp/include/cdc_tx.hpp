#pragma once

#include <string_view>

/** Stable numeric tx id from hex token (LSN, Xid prefix, lsid, etc.). */
long long cdc_tx_id_from_hex(std::string_view hex);
