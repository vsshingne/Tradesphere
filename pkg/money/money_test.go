package money

import "testing"

func TestMoneyRoundTripsExactly(t *testing.T) {
	value, err := MoneyFromDecimal("1025.125")
	if err != nil {
		t.Fatalf("MoneyFromDecimal failed: %v", err)
	}

	if got := MoneyToDecimal(value); got != "1025.12500000" {
		t.Fatalf("unexpected decimal output: %s", got)
	}
}

func TestCostForUsesExactScaledArithmetic(t *testing.T) {
	price, err := MoneyFromDecimal("50000")
	if err != nil {
		t.Fatalf("parse price: %v", err)
	}
	quantity, err := QuantityFromDecimal("1.5")
	if err != nil {
		t.Fatalf("parse quantity: %v", err)
	}

	total, err := CostFor(price, quantity)
	if err != nil {
		t.Fatalf("CostFor failed: %v", err)
	}

	if got := MoneyToDecimal(total); got != "75000.00000000" {
		t.Fatalf("unexpected cost: %s", got)
	}
}

func TestCostForRejectsSubAtomicValues(t *testing.T) {
	price, err := MoneyFromDecimal("0.00000001")
	if err != nil {
		t.Fatalf("parse price: %v", err)
	}
	quantity, err := QuantityFromDecimal("0.00000001")
	if err != nil {
		t.Fatalf("parse quantity: %v", err)
	}

	if _, err := CostFor(price, quantity); err == nil {
		t.Fatalf("expected sub-atomic cost to be rejected")
	}
}

func TestFormatDecimalMinInt64(t *testing.T) {
	// Ensure MinInt64 doesn't overflow negation in formatDecimal.
	result := formatDecimal(-9223372036854775808)
	if result == "" {
		t.Fatalf("expected non-empty result for MinInt64")
	}
}

func TestMustCostForPanicsOnError(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected MustCostFor to panic on sub-atomic input")
		}
	}()

	price, _ := MoneyFromDecimal("0.00000001")
	quantity, _ := QuantityFromDecimal("0.00000001")
	MustCostFor(price, quantity)
}

func TestMoneyJSONRoundTrip(t *testing.T) {
	original, _ := MoneyFromDecimal("12345.67890000")
	data, err := original.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}

	var parsed Money
	if err := parsed.UnmarshalJSON(data); err != nil {
		t.Fatalf("UnmarshalJSON: %v", err)
	}

	if parsed != original {
		t.Fatalf("JSON round-trip mismatch: got %v want %v", parsed, original)
	}
}

func TestQuantityJSONRoundTrip(t *testing.T) {
	original, _ := QuantityFromDecimal("2.50000000")
	data, err := original.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}

	var parsed Quantity
	if err := parsed.UnmarshalJSON(data); err != nil {
		t.Fatalf("UnmarshalJSON: %v", err)
	}

	if parsed != original {
		t.Fatalf("JSON round-trip mismatch: got %v want %v", parsed, original)
	}
}
