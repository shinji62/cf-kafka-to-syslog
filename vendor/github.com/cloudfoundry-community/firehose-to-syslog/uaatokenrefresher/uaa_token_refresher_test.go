package uaatokenrefresher_test

import (
	. "github.com/cloudfoundry-community/firehose-to-syslog/uaatokenrefresher"
	"github.com/cloudfoundry-community/firehose-to-syslog/uaatokenrefresher/fakes"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("UAATokenRefresher", func() {
	var (
		err       error
		fakeToken string

		fakeUAA            *fakes.FakeUAA
		authTokenRefresher *UAATokenRefresher
	)

	BeforeEach(func() {
		fakeUAA = fakes.NewFakeUAA("bearer", "123456789")
		fakeToken = fakeUAA.AuthToken()
		fakeUAA.Start()

		authTokenRefresher, err = NewUAATokenRefresher(
			fakeUAA.URL(), "client-id", "client-secret", true,
		)
	})

	AfterEach(func() {
		fakeUAA.Close()
	})

	It("fetches a token from the UAA", func() {
		authToken, err := authTokenRefresher.RefreshAuthToken()
		Expect(fakeUAA.Requested()).To(BeTrue())
		Expect(authToken).To(Equal(fakeToken))
		Expect(err).ToNot(HaveOccurred())
	})
})
