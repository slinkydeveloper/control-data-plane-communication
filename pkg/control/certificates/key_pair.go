package certificates

import "encoding/pem"

type KeyPair struct {
	privateKeyBlock    *pem.Block
	privateKeyPemBytes []byte

	certBlock *pem.Block
	certBytes []byte
}

func NewKeyPair(privateKey *pem.Block, cert *pem.Block) *KeyPair {
	return &KeyPair{
		privateKeyBlock:    privateKey,
		privateKeyPemBytes: pem.EncodeToMemory(privateKey),
		certBlock:          cert,
		certBytes:          pem.EncodeToMemory(cert),
	}
}

func (kh *KeyPair) PrivateKey() *pem.Block {
	return kh.privateKeyBlock
}

func (kh *KeyPair) PrivateKeyBytes() []byte {
	return kh.privateKeyPemBytes
}

func (kh *KeyPair) Cert() *pem.Block {
	return kh.certBlock
}

func (kh *KeyPair) CertBytes() []byte {
	return kh.certBytes
}
