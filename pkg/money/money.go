// Package money provides fixed-precision arithmetic for financial values.
//
// Both prices (Money) and quantities (Quantity) are stored as int64 with
// 8 decimal places of precision (Scale = 1e8).
//
// Examples:
//
//	₹100.25   → Money(10025000000)
//	1.5 units → Quantity(150000000)
package money

import (
	"encoding/json"
	"fmt"
	"math"
	"math/big"
	"strings"
)

const (
	// Scale is the fixed-precision multiplier: 1.00000000 = 100_000_000
	Scale int64 = 100000000

	// Decimals is the number of decimal places supported.
	Decimals = 8
)

// Money represents a monetary amount in the smallest unit (e.g. paise).
// Stored as int64 scaled by 1e8.
type Money int64

// Quantity represents a security quantity with 8 decimal places of precision.
// Stored as int64 scaled by 1e8.
type Quantity int64

// =====================================================
// Constructors
// =====================================================

// MoneyFromDecimal converts a decimal string to Money.
//
//	"100.25" → Money(10025000000)
func MoneyFromDecimal(text string) (Money, error) {
	value, err := parseDecimal(text)
	if err != nil {
		return 0, err
	}
	return Money(value), nil
}

// QuantityFromDecimal converts a decimal string to Quantity.
//
//	"1.5" → Quantity(150000000)
func QuantityFromDecimal(text string) (Quantity, error) {
	value, err := parseDecimal(text)
	if err != nil {
		return 0, err
	}
	return Quantity(value), nil
}

// =====================================================
// Formatting
// =====================================================

// MoneyToDecimal converts Money to its canonical decimal string.
//
//	Money(10025000000) → "100.25000000"
func MoneyToDecimal(value Money) string {
	return formatDecimal(int64(value))
}

// QuantityToDecimal converts Quantity to its canonical decimal string.
//
//	Quantity(150000000) → "1.50000000"
func QuantityToDecimal(value Quantity) string {
	return formatDecimal(int64(value))
}

// =====================================================
// Arithmetic
// =====================================================

// CostFor computes price * quantity using exact scaled arithmetic.
// Returns an error if the result cannot be represented exactly.
func CostFor(price Money, quantity Quantity) (Money, error) {
	value, err := multiplyScaled(int64(price), int64(quantity))
	if err != nil {
		return 0, err
	}
	return Money(value), nil
}

// MustCostFor computes price * quantity and panics on error.
// Only use when prior validation guarantees exactness.
func MustCostFor(price Money, quantity Quantity) Money {
	value, err := CostFor(price, quantity)
	if err != nil {
		panic(err)
	}
	return value
}

// =====================================================
// JSON Serialization
//
// Wire format is always a quoted decimal string:
//
//	{"price": "100.25000000"}
//
// Accepts both quoted strings and raw JSON numbers on input.
// =====================================================

// MarshalJSON serializes Money as a quoted decimal string.
func (m Money) MarshalJSON() ([]byte, error) {
	return json.Marshal(MoneyToDecimal(m))
}

// UnmarshalJSON deserializes Money from a quoted decimal string or raw number.
func (m *Money) UnmarshalJSON(data []byte) error {
	text, err := decodeJSONDecimal(data)
	if err != nil {
		return err
	}
	value, err := MoneyFromDecimal(text)
	if err != nil {
		return err
	}
	*m = value
	return nil
}

// MarshalJSON serializes Quantity as a quoted decimal string.
func (q Quantity) MarshalJSON() ([]byte, error) {
	return json.Marshal(QuantityToDecimal(q))
}

// UnmarshalJSON deserializes Quantity from a quoted decimal string or raw number.
func (q *Quantity) UnmarshalJSON(data []byte) error {
	text, err := decodeJSONDecimal(data)
	if err != nil {
		return err
	}
	value, err := QuantityFromDecimal(text)
	if err != nil {
		return err
	}
	*q = value
	return nil
}

// =====================================================
// Internal Helpers
// =====================================================

func decodeJSONDecimal(data []byte) (string, error) {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" || trimmed == "null" {
		return "", fmt.Errorf("decimal value is required")
	}
	// Quoted JSON string: "100.25"
	if trimmed[0] == '"' {
		var text string
		if err := json.Unmarshal(data, &text); err != nil {
			return "", err
		}
		return text, nil
	}
	// Raw JSON number: 100.25
	return trimmed, nil
}

func parseDecimal(text string) (int64, error) {
	trimmed := strings.TrimSpace(text)
	if trimmed == "" {
		return 0, fmt.Errorf("decimal value is required")
	}

	sign := int64(1)
	switch trimmed[0] {
	case '-':
		sign = -1
		trimmed = trimmed[1:]
	case '+':
		trimmed = trimmed[1:]
	}

	if trimmed == "" {
		return 0, fmt.Errorf("invalid decimal value %q", text)
	}

	parts := strings.Split(trimmed, ".")
	if len(parts) > 2 {
		return 0, fmt.Errorf("invalid decimal value %q", text)
	}

	whole := parts[0]
	fraction := ""
	if len(parts) == 2 {
		fraction = parts[1]
	}

	if whole == "" {
		whole = "0"
	}

	if !isDigits(whole) || !isDigits(fraction) {
		return 0, fmt.Errorf("invalid decimal value %q", text)
	}

	if len(fraction) > Decimals {
		return 0, fmt.Errorf("value %q exceeds %d decimal places", text, Decimals)
	}

	// Right-pad fraction to exactly Decimals digits.
	fraction += strings.Repeat("0", Decimals-len(fraction))
	combined := whole + fraction

	number := new(big.Int)
	if _, ok := number.SetString(combined, 10); !ok {
		return 0, fmt.Errorf("invalid decimal value %q", text)
	}
	if sign < 0 {
		number.Neg(number)
	}
	if !number.IsInt64() {
		return 0, fmt.Errorf("value %q overflows int64", text)
	}

	return number.Int64(), nil
}

func formatDecimal(value int64) string {
	// Prevent MinInt64 negation overflow.
	if value == math.MinInt64 {
		return "-92233720368.54775808"
	}

	sign := ""
	if value < 0 {
		sign = "-"
		value = -value
	}

	whole := value / Scale
	fraction := value % Scale
	return fmt.Sprintf("%s%d.%0*d", sign, whole, Decimals, fraction)
}

func multiplyScaled(left, right int64) (int64, error) {
	product := new(big.Int).Mul(big.NewInt(left), big.NewInt(right))
	scale := big.NewInt(Scale)
	quotient, remainder := new(big.Int), new(big.Int)
	quotient.QuoRem(product, scale, remainder)

	if remainder.Sign() != 0 {
		return 0, fmt.Errorf("value cannot be represented exactly at %d decimal places", Decimals)
	}
	if !quotient.IsInt64() {
		return 0, fmt.Errorf("scaled multiplication overflows int64")
	}

	return quotient.Int64(), nil
}

func isDigits(text string) bool {
	for _, ch := range text {
		if ch < '0' || ch > '9' {
			return false
		}
	}
	return true
}
