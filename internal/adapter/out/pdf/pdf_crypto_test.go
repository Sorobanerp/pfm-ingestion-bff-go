package pdf

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPDFCrypto_InvalidBytes(t *testing.T) {
	// Encrypt invalid PDF bytes returns error
	_, err := EncryptPDFInMemory([]byte("not a valid pdf"), "password123")
	assert.Error(t, err)

	// Decrypt invalid PDF bytes returns error
	_, err = unlockPDFInMemory([]byte("not a valid pdf"), "password123")
	assert.Error(t, err)
}
