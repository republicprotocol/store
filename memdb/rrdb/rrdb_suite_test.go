package rrdb_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"testing"
)

func TestRrdb(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Rrdb Suite")
}
