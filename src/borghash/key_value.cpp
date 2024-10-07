// KeyValue is a flexible, but memory-efficient, dense way to store k/v pairs.
// Usage: KeyValue<KeyLenBytes, ValueLenBytes>

#include <cstdint>
#include <cstddef>

template<std::size_t KeyLen, std::size_t ValueLen>
class KeyValue {
public:
    union Key {
        uint8_t asBytes[KeyLen];
    };

    union Value {
        uint8_t asBytes[ValueLen];
    };

    KeyValue() {
        for (std::size_t i = 0; i < KeyLen; ++i) key.asBytes[i] = 0;
        for (std::size_t i = 0; i < ValueLen; ++i) value.asBytes[i] = 0;
    }

    KeyValue(const uint8_t(&init_key)[KeyLen], const uint8_t(&init_value)[ValueLen]) {
        for (std::size_t i = 0; i < KeyLen; ++i) key.asBytes[i] = init_key[i];
        for (std::size_t i = 0; i < ValueLen; ++i) value.asBytes[i] = init_value[i];
    }

    const uint8_t* getKey() const {
        return key.asBytes;
    }

    const uint8_t* getValue() const {
        return value.asBytes;
    }

    void setKey(const uint8_t(&newKey)[KeyLen]) {
        for (std::size_t i = 0; i < KeyLen; ++i) key.asBytes[i] = newKey[i];
    }

    void setValue(const uint8_t(&newValue)[ValueLen]) {
        for (std::size_t i = 0; i < ValueLen; ++i) value.asBytes[i] = newValue[i];
    }

private:
    Key key;
    Value value;
};
