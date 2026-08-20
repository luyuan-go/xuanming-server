package plannerdb

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"fmt"
	"io"
	"math/big"
)

const crockfordAlphabet = "0123456789abcdefghjkmnpqrstvwxyz"

func generateWorkspaceID(random io.Reader) (string, error) {
	raw := make([]byte, 16)
	if _, err := io.ReadFull(random, raw); err != nil {
		return "", fmt.Errorf("生成 workspace ID: %w", err)
	}
	number := new(big.Int).SetBytes(raw)
	encoded := make([]byte, 26)
	base := big.NewInt(32)
	remainder := new(big.Int)
	for i := len(encoded) - 1; i >= 0; i-- {
		number.QuoRem(number, base, remainder)
		encoded[i] = crockfordAlphabet[remainder.Int64()]
	}
	return string(encoded), nil
}

func generatePassword(random io.Reader) (string, error) {
	raw := make([]byte, 32)
	if _, err := io.ReadFull(random, raw); err != nil {
		return "", fmt.Errorf("生成数据库凭据: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func newGCM(masterKey []byte) (cipher.AEAD, error) {
	if len(masterKey) != 32 {
		return nil, fmt.Errorf("master key 必须恰好 32 字节，实为 %d", len(masterKey))
	}
	block, err := aes.NewCipher(masterKey)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

func secretAAD(workspaceID, role string, version uint64) []byte {
	return fmt.Appendf(nil, "pandora/plannerdb/v1/%s/%s/%d", workspaceID, role, version)
}

func sealSecret(gcm cipher.AEAD, random io.Reader, workspaceID, role string, version uint64, plaintext string) (SecretBox, error) {
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(random, nonce); err != nil {
		return SecretBox{}, fmt.Errorf("生成凭据 nonce: %w", err)
	}
	ciphertext := gcm.Seal(nil, nonce, []byte(plaintext), secretAAD(workspaceID, role, version))
	return SecretBox{Nonce: nonce, Ciphertext: ciphertext}, nil
}

func openSecret(gcm cipher.AEAD, workspaceID, role string, version uint64, box SecretBox) (string, error) {
	plaintext, err := gcm.Open(nil, box.Nonce, box.Ciphertext, secretAAD(workspaceID, role, version))
	if err != nil {
		return "", errorsCredentialCiphertext
	}
	return string(plaintext), nil
}

var errorsCredentialCiphertext = fmt.Errorf("workspace 凭据密文认证失败")
