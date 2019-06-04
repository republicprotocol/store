package rrdb

import (
	"bytes"
	"fmt"
	"testing/quick"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/renproject/kv/db"
)

var _ = Describe("in-memory implementation of the db with random-replacement", func() {
	Context("when reading and writing", func() {
		It("should be able read and write value", func() {
			readAndWrite := func(key string, value []byte) bool {
				rrDB := New(10)
				if key == "" {
					return true
				}

				// Expect not value exists in the db with the given key.
				_, err := rrDB.Get(key)
				Expect(err).Should(Equal(db.ErrNotFound))

				// Should be able to read the value after inserting.
				Expect(rrDB.Insert(key, value)).NotTo(HaveOccurred())
				data, err := rrDB.Get(key)
				Expect(err).NotTo(HaveOccurred())
				Expect(bytes.Compare(data, value)).Should(BeZero())

				// Expect no value exists after deleting the value.
				Expect(rrDB.Delete(key)).NotTo(HaveOccurred())
				_, err = rrDB.Get(key)
				return err == db.ErrNotFound
			}

			Expect(quick.Check(readAndWrite, nil)).NotTo(HaveOccurred())
		})

		It("should be able to iterable through the db using the iterator", func() {
			iteration := func(values [][]byte) bool {
				rrDB := New(len(values))

				// Insert all values and make a map for validation.
				allValues := map[string][]byte{}
				for i, value := range values {
					key := fmt.Sprintf("%v", i)
					Expect(rrDB.Insert(key, value)).NotTo(HaveOccurred())
					allValues[key] = value
				}

				// Expect db size to the number of values we insert.
				size, err := rrDB.Size()
				Expect(err).NotTo(HaveOccurred())
				Expect(size).Should(Equal(len(values)))

				// Expect iterator gives us all the key-value pairs we insert.
				iter := rrDB.Iterator()
				for iter.Next() {
					key, err := iter.Key()
					Expect(err).NotTo(HaveOccurred())
					value, err := iter.Value()
					Expect(err).NotTo(HaveOccurred())

					stored, ok := allValues[key]
					Expect(ok).Should(BeTrue())
					Expect(bytes.Compare(value, stored)).Should(BeZero())
					delete(allValues, key)
				}
				return len(allValues) == 0
			}

			Expect(quick.Check(iteration, nil)).NotTo(HaveOccurred())
		})

		It("should be able to add new elements past the capacity without increasing the size", func() {
			iteration := func(values [][]byte) bool {
				cap := len(values) / 2
				if cap < 1 {
					cap = 1
				}
				rrDB := New(cap)

				// Insert all values.
				for i, value := range values {
					key := fmt.Sprintf("%v", i)
					Expect(rrDB.Insert(key, value)).NotTo(HaveOccurred())
					val, err := rrDB.Get(key)
					Expect(err).ShouldNot(HaveOccurred())
					Expect(val).To(Equal(value))
				}

				// Expect db size to be max capacity.
				size, err := rrDB.Size()
				Expect(err).NotTo(HaveOccurred())
				if len(values) > 0 {
					Expect(size).Should(Equal(cap))
				}
				return true
			}

			Expect(quick.Check(iteration, nil)).NotTo(HaveOccurred())
		})

		It("should return error when trying to get key/value when the iterator doesn't have next value", func() {
			iteration := func(key string, value []byte) bool {
				rrDB := New(10)
				iter := rrDB.Iterator()

				for iter.Next() {
				}

				_, err := iter.Key()
				Expect(err).Should(Equal(db.ErrIndexOutOfRange))
				_, err = iter.Value()
				Expect(err).Should(Equal(db.ErrIndexOutOfRange))

				return iter.Next() == false
			}

			Expect(quick.Check(iteration, nil)).NotTo(HaveOccurred())
		})

		It("should return error when trying to get key/value without calling Next()", func() {
			iteration := func(key string, value []byte) bool {
				rrDB := New(10)
				iter := rrDB.Iterator()

				_, err := iter.Key()
				Expect(err).Should(Equal(db.ErrIndexOutOfRange))
				_, err = iter.Value()
				Expect(err).Should(Equal(db.ErrIndexOutOfRange))

				return iter.Next() == false
			}

			Expect(quick.Check(iteration, nil)).NotTo(HaveOccurred())
		})

		It("should return ErrEmptyKey when trying to insert a value with empty key", func() {
			iteration := func(value []byte) bool {
				rrDB := New(10)
				return rrDB.Insert("", value) == db.ErrEmptyKey
			}

			Expect(quick.Check(iteration, nil)).NotTo(HaveOccurred())
		})
	})
})
