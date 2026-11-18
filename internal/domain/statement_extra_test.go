package domain

import "testing"

func TestParseExactDecimal_Table(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    int64
		wantErr bool
	}{
		{"empty", "  ", 0, true},
		{"invalid", "abc", 0, true},
		{"canonical", "1234.56", 12345600, false},
		{"brazilian both separators", "1.234,56", 12345600, false},
		{"brazilian comma only", "45,9", 459000, false},
		{"negative", "-45.9", -459000, false},
		{"round half even to zero", "0.00005", 0, false},
		{"round half even stays", "0.00015", 2, false},
		{"round half even down", "0.00025", 2, false},
		{"round above half positive", "0.00006", 1, false},
		{"round above half negative", "-0.00006", -1, false},
		{"below half", "0.00004", 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseExactDecimal(tc.in)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err=%v wantErr=%v", err, tc.wantErr)
			}
			if err == nil && got.Scaled() != tc.want {
				t.Fatalf("got %d want %d", got.Scaled(), tc.want)
			}
		})
	}
}

func TestExactDecimal_Operations(t *testing.T) {
	a := NewExactDecimalFromScaled(15000)
	b := NewExactDecimalFromScaled(5000)
	if got := a.Add(b).Scaled(); got != 20000 {
		t.Fatalf("add=%d", got)
	}
	if got := a.Sub(b).Scaled(); got != 10000 {
		t.Fatalf("sub=%d", got)
	}
	if got := a.Neg().Scaled(); got != -15000 {
		t.Fatalf("neg=%d", got)
	}
	if !a.Neg().IsNegative() || a.IsNegative() {
		t.Fatal("IsNegative wrong")
	}
	if !NewExactDecimalFromScaled(0).IsZero() || a.IsZero() {
		t.Fatal("IsZero wrong")
	}
	if !a.Equal(NewExactDecimalFromScaled(15000)) || a.Equal(b) {
		t.Fatal("Equal wrong")
	}
	fixed := map[int64]string{15000: "1.5000", -459000: "-45.9000", 1: "0.0001", 0: "0.0000"}
	for in, want := range fixed {
		if got := NewExactDecimalFromScaled(in).Fixed4(); got != want {
			t.Fatalf("Fixed4(%d)=%s want %s", in, got, want)
		}
	}
}

func TestComputeIdempotencyKey_InstallmentDefaults(t *testing.T) {
	amt := NewExactDecimalFromScaled(10000)
	def := ComputeIdempotencyKey("t", "1234", "2025-01-01", amt, "  shop  x ", 0, -1)
	exp := ComputeIdempotencyKey("t", "1234", "2025-01-01", amt, "SHOP X", 1, 1)
	if def != exp {
		t.Fatalf("defaults not normalized: %s vs %s", def, exp)
	}
	if def == ComputeIdempotencyKey("t", "1234", "2025-01-01", amt, "SHOP X", 2, 3) {
		t.Fatal("different installments must differ")
	}
}

func TestNewStatement_Branches(t *testing.T) {
	amt := NewExactDecimalFromScaled(10000)
	t.Run("blank tenant", func(t *testing.T) {
		if _, err := NewStatement("s", " ", IssuerItau, "1", "2025-01", "", "", amt, nil); err != ErrEmptyTenantID {
			t.Fatalf("err=%v", err)
		}
	})
	t.Run("defaults and valid", func(t *testing.T) {
		txs := []RawTransaction{
			{RawDescription: " padaria  x ", Amount: amt, CompetenceDate: "2025-01-02"},
			{MerchantNormalized: "loja", CardLast4: "9999", Currency: "USD", Amount: amt, CurrentInstallment: 2, TotalInstallments: 3},
		}
		st, err := NewStatement("s", "t", IssuerBB, "1234", "2025-01", "2025-02-01", "", amt.Add(amt), txs)
		if err != nil {
			t.Fatal(err)
		}
		if st.Currency != "BRL" || !st.Checksum.IsValid {
			t.Fatalf("bad stmt %+v", st.Checksum)
		}
		t0, t1 := st.Transactions[0], st.Transactions[1]
		if t0.MerchantNormalized != "PADARIA X" || t0.CardLast4 != "1234" || t0.Currency != "BRL" || t0.CurrentInstallment != 1 || t0.TotalInstallments != 1 {
			t.Fatalf("defaults wrong %+v", t0)
		}
		if t1.CardLast4 != "9999" || t1.Currency != "USD" || t1.MerchantNormalized != "LOJA" || t1.CurrentInstallment != 2 {
			t.Fatalf("overrides wrong %+v", t1)
		}
		if t0.IdempotencyKey == "" || t0.IdempotencyKey == t1.IdempotencyKey { //gitleaks:allow
			t.Fatal("idempotency keys invalid")
		}
	})
	t.Run("mismatch returns stmt and error", func(t *testing.T) {
		st, err := NewStatement("s", "t", IssuerMarisa, "1", "2025-01", "", "BRL", amt, nil)
		if err == nil || st == nil || st.Checksum.IsValid {
			t.Fatalf("expected mismatch, st=%v err=%v", st, err)
		}
	})
}
