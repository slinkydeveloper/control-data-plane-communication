package controlprotocol

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
)

const (
	bitSize = 2048
)

var randReader = rand.Reader

func generateKey() (*pem.Block, *pem.Block, error) {
	k, err := rsa.GenerateKey(randReader, bitSize)
	if err != nil {
		return nil, nil, err
	}

	privateKeyPem := &pem.Block{
		Type:  "PRIVATE KEY",
		Bytes: x509.MarshalPKCS1PrivateKey(k),
	}

	publicKeyPem := &pem.Block{
		Type:  "PUBLIC KEY",
		Bytes: x509.MarshalPKCS1PublicKey(&k.PublicKey),
	}

	return privateKeyPem, publicKeyPem, nil
}

type KeyHolder struct {
	privateKeyBlock    *pem.Block
	privateKeyPemBytes []byte

	publicKeyBlock    *pem.Block
	publicKeyPemBytes []byte
}

func GenerateKeyHolder() (*KeyHolder, error) {
	private, public, err := generateKey()
	if err != nil {
		return nil, err
	}
	keyHolder := KeyHolder{
		privateKeyBlock:    private,
		privateKeyPemBytes: pem.EncodeToMemory(private),
		publicKeyBlock:     public,
		publicKeyPemBytes:  pem.EncodeToMemory(public),
	}
	return &keyHolder, nil
}

func (kh *KeyHolder) PrivateKey() *pem.Block {
	return kh.privateKeyBlock
}

func (kh *KeyHolder) PrivateKeyBytes() []byte {
	return kh.privateKeyPemBytes
}

func (kh *KeyHolder) PublicKey() *pem.Block {
	return kh.publicKeyBlock
}

func (kh *KeyHolder) PublicKeyBytes() []byte {
	return kh.publicKeyPemBytes
}
