#include "cdc_tx.hpp"
#include "test_assert.hpp"

int main() {
    expect_true(cdc_tx_id_from_hex("0x3036") == cdc_tx_id_from_hex("3036"), "strip 0x prefix");
    expect_true(cdc_tx_id_from_hex("3036") == 0x3036LL, "numeric hex");
    expect_true(cdc_tx_id_from_hex("") == 0, "empty hex");
    expect_true(cdc_tx_id_from_hex("gg") == 0, "non-hex");
    return 0;
}
